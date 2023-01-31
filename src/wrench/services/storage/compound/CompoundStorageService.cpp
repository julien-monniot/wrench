#include <wrench/services/storage/compound/CompoundStorageService.h>
#include <wrench/services/ServiceMessage.h>
#include "wrench/services/storage/StorageServiceMessage.h"
#include <wrench/services/storage/StorageServiceMessagePayload.h>
#include <wrench/simgrid_S4U_util/S4U_Mailbox.h>
#include <wrench/failure_causes/FileNotFound.h>
#include <wrench/failure_causes/StorageServiceNotEnoughSpace.h>
#include <wrench/failure_causes/NotAllowed.h>
#include <wrench/exceptions/ExecutionException.h>
#include <wrench/logging/TerminalOutput.h>

WRENCH_LOG_CATEGORY(wrench_core_compound_storage_system,
                    "Log category for Compound Storage Service");

namespace wrench { 


    /** @brief Default StorageSelectionStrategyCallback: strategy used by the CompoundStorageService 
     *         when no strategy is provided at instanciation. By default, it returns a nullptr, which 
     *         trigger any request message processing function in CompoundStorageServer to answer negatively.
     *         
    */
    std::shared_ptr<FileLocation> nullptrStorageServiceSelection(
        const std::shared_ptr<DataFile>& file, 
        const std::set<std::shared_ptr<StorageService>>& resources,
        const std::map<std::shared_ptr<DataFile>, std::shared_ptr<FileLocation>>& mapping) {
        return nullptr;
    }


    /** @brief Constructor for the case where no request message (for I/O operations) should ever reach
     *         the CompoundStorageService. This use case suppose that any action making use of a FileLocation
     *         referencing this CompoundStorageService will be intercepted before its execution (in a scheduler
     *         for instance) and updated with one of the StorageServices known to this CompoundStorageService.
    */
    CompoundStorageService::CompoundStorageService(const std::string &hostname,
                                                   std::set<std::shared_ptr<StorageService>> storage_services,
                                                   WRENCH_PROPERTY_COLLECTION_TYPE property_list,
                                                   WRENCH_MESSAGE_PAYLOADCOLLECTION_TYPE messagepayload_list) :
            CompoundStorageService(hostname, storage_services, nullptrStorageServiceSelection, 
                                   property_list, messagepayload_list, "_" + std::to_string(getNewUniqueNumber())) {};


    /** @brief Constructor for the case where the user provides a callback (StorageSelectionStrategyCallback) 
     *         which will be used by the CompoundStorageService any time it receives a file write or file copy 
     *         request, in order to determine which underlying StorageService to use for the (potentially) new
     *         file in the request. 
     *         Note that nothing prevents the user from also intercepting some actions (see use case for other 
     *         constructor), but resulting behaviour is undefined.
    */
    CompoundStorageService::CompoundStorageService(const std::string &hostname,
                                                   std::set<std::shared_ptr<StorageService>> storage_services,
                                                   StorageSelectionStrategyCallback storage_selection,
                                                   WRENCH_PROPERTY_COLLECTION_TYPE property_list,
                                                   WRENCH_MESSAGE_PAYLOADCOLLECTION_TYPE messagepayload_list) :
            CompoundStorageService(hostname, storage_services, storage_selection, property_list, messagepayload_list, 
                                   "_" + std::to_string(getNewUniqueNumber())) {};



    CompoundStorageService::CompoundStorageService(
                const std::string &hostname, 
                std::set<std::shared_ptr<StorageService>> storage_services,
                StorageSelectionStrategyCallback storage_selection,
                WRENCH_PROPERTY_COLLECTION_TYPE property_list,
                WRENCH_MESSAGE_PAYLOADCOLLECTION_TYPE messagepayload_list,
                const std::string &suffix) : StorageService(hostname, 
                    "compound_storage" + suffix) {

            this->setProperties(this->default_property_values, std::move(property_list));
            this->setMessagePayloads(this->default_messagepayload_values, std::move(messagepayload_list));
            this->validateProperties();

            if (storage_services.empty()) {
                throw std::invalid_argument("Got an empty list of SimpleStorageServices for CompoundStorageService."
                                            "Must specify at least one valid SimpleStorageService");
            }

            if (std::any_of(storage_services.begin(), storage_services.end(), [](const auto& elem){ return elem == NULL; })) {
                throw std::invalid_argument("One of the SimpleStorageServices provided is not initialized");
            }

            if (std::any_of(storage_services.begin(), storage_services.end(), [](const auto& elem){ return elem->isBufferized(); })) {
                throw std::invalid_argument("CompoundStorageService can't deal with bufferized StorageServices");
            }

            // CSS should be non-bufferized, as it actually doesn't copy / transfer anything
            // and this allows it to receive message requests for copy (otherwise, src storage service might receive it)
            this->buffer_size = 0;
            this->storage_services = storage_services;
            this->storage_selection = storage_selection;
            this->file_systems[LogicalFileSystem::DEV_NULL] = LogicalFileSystem::createLogicalFileSystem(
                this->getHostname(), 
                this, 
                LogicalFileSystem::DEV_NULL, 
                this->getPropertyValueAsString(wrench::StorageServiceProperty::CACHING_BEHAVIOR)
            );
        }


    /**
     * @brief Main method of the daemon
     *
     * @return 0 on termination
     */
    int CompoundStorageService::main() {

        //TODO: Use another colour?
        TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_CYAN);
        std::string message = "Compound Storage Service " + this->getName() + "  starting on host " + this->getHostname();
        WRENCH_INFO("%s", message.c_str());

        // Init file system. There is always only one built-in LogicalFilesystem, with a DEV_NULL mount point.
        for (auto const& fs: this->file_systems) { fs.second->init(); };

        for (const auto &ss: this->storage_services) {
            message = " - " + ss->process_name + " on " + ss->getHostname();
            WRENCH_INFO("%s", message.c_str());
            // For more info, see directly the logs from the SimpleStorageServices themselves.
            for (const auto &mnt: ss->getMountPoints()) {
                message = "   - " + mnt;
                WRENCH_INFO("%s", message.c_str());
            }
        }

        /** Main loop **/
        bool comm_ptr_has_been_posted = false;
        simgrid::s4u::CommPtr comm_ptr;
        std::unique_ptr<SimulationMessage> simulation_message;
        while (true) {

            S4U_Simulation::computeZeroFlop();

            // Create an async recv if needed
            if (not comm_ptr_has_been_posted) {
                try {
                    comm_ptr = this->mailbox->get_async<void>((void **) (&(simulation_message)));
                } catch (simgrid::NetworkFailureException &e) {
                    // oh well
                    continue;
                }
                comm_ptr_has_been_posted = true;
            }

            // Create all activities to wait on (only emplace the communicator)
            std::vector<simgrid::s4u::ActivityPtr> pending_activities;
            pending_activities.emplace_back(comm_ptr);

            // Wait one activity (communication in this case) to complete
            int finished_activity_index;
            try {
                finished_activity_index = (int) simgrid::s4u::Activity::wait_any(pending_activities);
            } catch (simgrid::NetworkFailureException &e) {
                comm_ptr_has_been_posted = false;
                continue;
            } catch (std::exception &e) {
                continue;
            }

            // It's a communication
            if (finished_activity_index == 0) {
                comm_ptr_has_been_posted = false;
                if (not processNextMessage(simulation_message.get())) break;
            } else if (finished_activity_index == -1) {
                throw std::runtime_error("wait_any() returned -1. Not sure what to do with this. ");
            }
        }

        WRENCH_INFO("Compound Storage Service %s on host %s cleanly terminating!",
                    this->getName().c_str(),
                    S4U_Simulation::getHostName().c_str());

        return 0;
    }

    /**
     * @brief Process a received control message
     *
     * @return false if the daemon should terminate
     */
    bool CompoundStorageService::processNextMessage(SimulationMessage *message) {

        WRENCH_INFO("Got a [%s] message", message->getName().c_str());

        if (auto msg = dynamic_cast<ServiceStopDaemonMessage *>(message)) {
            return processStopDaemonRequest(msg->ack_mailbox);

        } else if (auto msg = dynamic_cast<StorageServiceFileDeleteRequestMessage *>(message)) {
            return processFileDeleteRequest(msg);

        } else if (auto msg = dynamic_cast<StorageServiceFileLookupRequestMessage *>(message)) {
            return processFileLookupRequest(msg);

        } else if (auto msg = dynamic_cast<StorageServiceFileWriteRequestMessage *>(message)) {
            return processFileWriteRequest(msg);

        } else if (auto msg = dynamic_cast<StorageServiceFileReadRequestMessage *>(message)) {
            return processFileReadRequest(msg);

        } else if (auto msg = dynamic_cast<StorageServiceFileCopyRequestMessage *>(message)) {
            return processFileCopyRequest(msg);

        } else {
            throw std::runtime_error(
                "CompoundStorageService::processNextMessage(): Unexpected [" + message->getName() + "] message." + 
                "This is only an abstraction layer and it can't be used as an actual storage service");
        }
    }

    std::shared_ptr<FileLocation> CompoundStorageService::lookupFileLocation(const std::shared_ptr<DataFile> &file) {
        
        WRENCH_DEBUG("lookupFileLocation: For file %s", file->getID().c_str());

        if (this->file_location_mapping.find(file) == this->file_location_mapping.end()) {
            WRENCH_DEBUG("lookupFileLocation: file %s NOT found", file->getID().c_str());
            return nullptr;
        } else {
            auto mapped_location = this->file_location_mapping[file];
            WRENCH_DEBUG("lookupFileLocation: file %s found, on storage service %s", 
                        mapped_location->getFile()->getID().c_str(), 
                        mapped_location->getStorageService()->getName().c_str()
            );
            return mapped_location;
        }
    }

    std::shared_ptr<FileLocation> CompoundStorageService::lookupFileLocation(const std::shared_ptr<FileLocation> &location) {
        return this->lookupFileLocation(location->getFile());
    }

    std::shared_ptr<FileLocation> CompoundStorageService::lookupOrDesignateStorageService(const std::shared_ptr<DataFile> concrete_file_location) {

        if (this->lookupFileLocation(concrete_file_location)) {
            WRENCH_DEBUG("lookupOrDesignateStorageService: File %s already known by CSS", concrete_file_location->getID().c_str());
            return this->file_location_mapping[concrete_file_location];
        }

        WRENCH_DEBUG("lookupOrDesignateStorageService: File %s NOT already known by CSS", concrete_file_location->getID().c_str());
        auto designatedLocation = this->storage_selection(concrete_file_location, this->storage_services, this->file_location_mapping);

        if (!designatedLocation) {
            WRENCH_DEBUG("lookupOrDesignateStorageService: File %s could not be placed on any ss", concrete_file_location->getID().c_str());
        } else {
            WRENCH_DEBUG("lookupOrDesignateStorageService: Registering file %s on storage service %s, at path %s", 
                        designatedLocation->getFile()->getID().c_str(),
                        designatedLocation->getStorageService()->getName().c_str(),
                        designatedLocation->getFullAbsolutePath().c_str()    
            );
            // Supposing (and it better be true) that DataFiles are unique throught a given simulation run, even among various jobs.
            this->file_location_mapping[designatedLocation->getFile()] = FileLocation::LOCATION(
                designatedLocation->getStorageService(), 
                designatedLocation->getFullAbsolutePath(), 
                designatedLocation->getFile()
            );
        }

        return designatedLocation;
    }

    std::shared_ptr<FileLocation> CompoundStorageService::lookupOrDesignateStorageService(const std::shared_ptr<FileLocation> location) {
        return this->lookupOrDesignateStorageService(location->getFile());
    }

    bool CompoundStorageService::processFileDeleteRequest(StorageServiceFileDeleteRequestMessage *msg) {

        auto designated_location = this->lookupFileLocation(msg->location);
        if (!designated_location) {

            WRENCH_WARN("processFileDeleteRequest: Unable to find file %s", 
                        msg->location->getFile()->getID().c_str());
            try {        
                S4U_Mailbox::dputMessage(
                    msg->answer_mailbox,
                    new StorageServiceFileDeleteAnswerMessage(
                            nullptr,
                            this->getSharedPtr<CompoundStorageService>(),
                            false,
                            std::shared_ptr<FailureCause>(new FileNotFound(msg->location)),
                            this->getMessagePayloadValue(
                                    CompoundStorageServiceMessagePayload::FILE_DELETE_ANSWER_MESSAGE_PAYLOAD)));
            } catch (wrench::ExecutionException &e) {}
        
            return true;
        }

        S4U_Mailbox::putMessage(
            designated_location->getStorageService()->mailbox,
            new StorageServiceFileDeleteRequestMessage(
                msg->answer_mailbox,
                designated_location,
                designated_location->getStorageService()->getMessagePayloadValue(
                    StorageServiceMessagePayload::FILE_DELETE_REQUEST_MESSAGE_PAYLOAD)
            )
        );

        return true;
    }
    
    bool CompoundStorageService::processFileLookupRequest(StorageServiceFileLookupRequestMessage* msg) {
        
        auto designated_location = this->lookupFileLocation(msg->location);
        if (!designated_location) {
            
            WRENCH_WARN("processFileLookupRequest: Unable to find file %s", 
                        msg->location->getFile()->getID().c_str());
            // Abort because we don't know the file (it should have been written or copied somewhere before the lookup happens)
            try {
                S4U_Mailbox::dputMessage(
                    msg->answer_mailbox,
                    new StorageServiceFileLookupAnswerMessage(
                        nullptr, false,
                        this->getMessagePayloadValue(
                            CompoundStorageServiceMessagePayload::FILE_LOOKUP_ANSWER_MESSAGE_PAYLOAD)));
            } catch (wrench::ExecutionException &e) {}
            
            return true;
        }

        // The file is known, we can forward the request to the underlying designated StorageService
        S4U_Mailbox::putMessage(
            designated_location->getStorageService()->mailbox,
            new StorageServiceFileLookupRequestMessage(
                msg->answer_mailbox,
                FileLocation::LOCATION(
                    designated_location->getStorageService(), 
                    designated_location->getFullAbsolutePath(), 
                    designated_location->getFile()
                ),
                designated_location->getStorageService()->getMessagePayloadValue(
                    StorageServiceMessagePayload::FILE_LOOKUP_REQUEST_MESSAGE_PAYLOAD))
        );

        return true;

    }

    bool CompoundStorageService::processFileCopyRequest(StorageServiceFileCopyRequestMessage *msg) {

        auto final_src = msg->src;
        auto final_dst = msg->dst;

        // If source location references a CSS, it must already be known to the CSS
        if (std::dynamic_pointer_cast<CompoundStorageService>(msg->src->getStorageService())) {
            final_src = this->lookupFileLocation(msg->src->getFile()); 
        }
        // If destination location references a CSS, it must already exist OR we must be able to allocate it
        if (std::dynamic_pointer_cast<CompoundStorageService>(msg->dst->getStorageService())) {
            final_dst = this->lookupOrDesignateStorageService(msg->dst);
        }

        // Error case - src
        if (!final_src) {
           
            WRENCH_WARN("processFileCopyRequest: Source %s is a CompoundStorageService and file doesn't exist yet.", 
                        msg->src->getStorageService()->getName().c_str());
            try {
                std::string error = "CompoundStorageService can't be the source of a file copy if the file has"
                                    " not already been written or copied to it.";
                S4U_Mailbox::putMessage(
                    msg->answer_mailbox,
                    new StorageServiceFileCopyAnswerMessage(
                        msg->src,
                        msg->dst,
                        nullptr, 
                        false,
                        false,
                        std::shared_ptr<FailureCause>(new NotAllowed(
                            this->getSharedPtr<CompoundStorageService>(),
                            error)
                        ),
                        this->getMessagePayloadValue(
                            CompoundStorageServiceMessagePayload::FILE_COPY_ANSWER_MESSAGE_PAYLOAD)
                    )
                );
            } catch (ExecutionException &e) {}

            return true;
        }

        // Error case - dst
        if (!final_dst) {

            WRENCH_WARN("processFileCopyRequest: Destination file %s not found or not enough space left",
                        msg->dst->getFile()->getID().c_str());
            try {
                S4U_Mailbox::putMessage(
                    msg->answer_mailbox,
                    new StorageServiceFileCopyAnswerMessage(
                        msg->src,
                        msg->dst,
                        nullptr, 
                        false,
                        false,
                        std::shared_ptr<FailureCause>(new FileNotFound(msg->src)),
                        this->getMessagePayloadValue(
                            CompoundStorageServiceMessagePayload::FILE_COPY_ANSWER_MESSAGE_PAYLOAD))
                );
            } catch (ExecutionException &e) {}

            return true;
        }
        
        S4U_Mailbox::putMessage(
            final_dst->getStorageService()->mailbox,
            new StorageServiceFileCopyRequestMessage(
                    msg->answer_mailbox,
                    final_src,
                    final_dst,
                    nullptr,
                    final_dst->getStorageService()->getMessagePayloadValue(
                            StorageServiceMessagePayload::FILE_COPY_REQUEST_MESSAGE_PAYLOAD)
            )
        );

        return true;
    }

    /**
     * @brief Send a synchronous write request for file on one of the underlying storage service
     *
     * @param file: the file
     * @param path: path to file
     *
     * @throw ExecutionException
     */
    void CompoundStorageService::writeFile(const std::shared_ptr<DataFile> &file, const std::string &path) {

        if (file == nullptr) {
            throw std::invalid_argument("CompoundStorageService::writeFile(): Invalid arguments");
        }

        auto designated_location = this->lookupOrDesignateStorageService(file);
        if (!designated_location) {
            throw StorageServiceNotEnoughSpace(file, this->getSharedPtr<CompoundStorageService>());
        }

        assertServiceIsUp();

        WRENCH_INFO("CompoundStorageService::writeFile: Preparing initial write request to underlying storage service");

        // Send a  message to the daemon
        auto answer_mailbox = S4U_Daemon::getRunningActorRecvMailbox();
        auto target_buffer_size = designated_location->getStorageService()->buffer_size;

        S4U_Mailbox::putMessage(
            designated_location->getStorageService()->mailbox,
            new StorageServiceFileWriteRequestMessage(
                answer_mailbox,
                simgrid::s4u::this_actor::get_host(),
                designated_location,
                target_buffer_size,
                designated_location->getStorageService()->getMessagePayloadValue(
                    StorageServiceMessagePayload::FILE_WRITE_REQUEST_MESSAGE_PAYLOAD)
            )
        );

        // Wait for a reply
        std::shared_ptr<SimulationMessage> message;

        WRENCH_INFO("writeFile: Waiting for answer message");
        message = S4U_Mailbox::getMessage(answer_mailbox, this->network_timeout);
        WRENCH_INFO("writeFile: Answer message received");

        if (auto msg = dynamic_cast<StorageServiceFileWriteAnswerMessage *>(message.get())) {
            WRENCH_INFO("writeFile: msg successfully cast to AnswerMessage");
            // If not a success, throw an exception
            if (not msg->success) {
                throw ExecutionException(msg->failure_cause);
            }

            if (target_buffer_size < 1) {
                WRENCH_INFO("writeFile: if buffer_size < 1");
                // just wait for the final ack (no timeout!)
                message = S4U_Mailbox::getMessage(answer_mailbox);
                if (not dynamic_cast<StorageServiceAckMessage *>(message.get())) {
                    throw std::runtime_error("CompoundStorageService::writeFile(): Received an unexpected [" +
                                             message->getName() + "] message instead of final ack!");
                }

            } else {
                WRENCH_INFO("writeFile: else");
                // Bufferized
                double remaining = file->getSize();
                while (remaining - target_buffer_size > DBL_EPSILON) {
                    S4U_Mailbox::putMessage(msg->data_write_mailbox,
                                            new StorageServiceFileContentChunkMessage(
                                                    file, target_buffer_size, false));
                    remaining -= target_buffer_size;
                }
                S4U_Mailbox::putMessage(msg->data_write_mailbox, new StorageServiceFileContentChunkMessage(
                                                                         file, remaining, true));

                //Waiting for the final ack
                message = S4U_Mailbox::getMessage(answer_mailbox, this->network_timeout);
                if (not dynamic_cast<StorageServiceAckMessage *>(message.get())) {
                    throw std::runtime_error("CompoundStorageService::writeFile(): Received an unexpected [" +
                                             message->getName() + "] message instead of final ack!");
                }
            }

        } else {
            throw std::runtime_error("CompoundStorageService::writeFile(): Received a totally unexpected [" +
                                     message->getName() + "] message!");
        }
    }

    bool CompoundStorageService::processFileWriteRequest(StorageServiceFileWriteRequestMessage* msg) {

        WRENCH_WARN("CompoundStorageService::processFileWriteRequest: UNREACHABLE CODE");

        auto designated_location = this->lookupOrDesignateStorageService(msg->location);
        if (!designated_location) {

            WRENCH_WARN("processFileWriteRequest: Destination file %s not found or not enough space left",
                        msg->location->getFile()->getID().c_str());
            try {
                S4U_Mailbox::dputMessage(
                    msg->answer_mailbox,
                    new StorageServiceFileWriteAnswerMessage(
                            msg->location,
                            false,
                            std::shared_ptr<FailureCause>(new FileNotFound(msg->location)),
                            nullptr,
                            this->getMessagePayloadValue(
                                    CompoundStorageServiceMessagePayload::FILE_WRITE_ANSWER_MESSAGE_PAYLOAD)));
            } catch (wrench::ExecutionException &e) {}
            
            return true;
        }

        // The file is known or added to the local mapping, we can forward the request to the underlying designated StorageService
        S4U_Mailbox::putMessage(
            designated_location->getStorageService()->mailbox,
            new StorageServiceFileWriteRequestMessage(
                msg->answer_mailbox,
                msg->requesting_host,
                designated_location,
                0,
                this->getMessagePayloadValue(
                    StorageServiceMessagePayload::FILE_WRITE_REQUEST_MESSAGE_PAYLOAD)
            )
        );

        return true;
    }

    bool CompoundStorageService::processFileReadRequest(StorageServiceFileReadRequestMessage* msg) {        
        
        auto designated_location = this->lookupFileLocation(msg->location);
        if (!designated_location) {

            WRENCH_WARN("processFileReadRequest: file %s not found", msg->location->getFile()->getID().c_str());
            try {
                S4U_Mailbox::dputMessage(
                    msg->answer_mailbox,
                    new StorageServiceFileReadAnswerMessage(
                        msg->location,
                        false,
                        std::shared_ptr<FailureCause>(new FileNotFound(msg->location)),
                        0,
                        this->getMessagePayloadValue(
                            CompoundStorageServiceMessagePayload::FILE_READ_ANSWER_MESSAGE_PAYLOAD))
                );
            } catch (wrench::ExecutionException &e) {}

            return true;

        }

        WRENCH_DEBUG("processFileReadRequest: Going to read file %s on storage service %s, at path %s", 
                        designated_location->getFile()->getID().c_str(),
                        designated_location->getStorageService()->getName().c_str(),
                        designated_location->getFullAbsolutePath().c_str()    
        );        

        // WRENCH_DEBUG("Mailbox destination (for recv file content) : %s", msg->mailbox_to_receive_the_file_content->get_cname());

        S4U_Mailbox::putMessage(
            designated_location->getStorageService()->mailbox,
            new StorageServiceFileReadRequestMessage(
                msg->answer_mailbox,
                msg->requesting_host,
                msg->mailbox_to_receive_the_file_content,
                designated_location,
                msg->num_bytes_to_read,
                designated_location->getStorageService()->getMessagePayloadValue(
                        StorageServiceMessagePayload::FILE_READ_REQUEST_MESSAGE_PAYLOAD)
            )
        );

        return true;
    }


    /**
     * @brief Get the load (number of concurrent reads) on the storage service
     *        Not implemented yet for CompoundStorageService (is it needed?)
     * @return the load on the service (currently throws)
     */
    double CompoundStorageService::getLoad() {
        WRENCH_WARN("CompoundStorageService::getLoad Not implemented");
        throw std::logic_error("CompoundStorageService::getLoad(): Not implemented. "
                                 "Call getLoad() on internal storage service(s) instead");
    }

    /**
     * @brief Get the total space across all internal services known by the CompoundStorageService
     * 
     * @return A map of service name and total capacity of all disks for each service.
     */
    std::map<std::string, double> CompoundStorageService::getTotalSpace() {

        WRENCH_INFO("CompoundStorageService::getTotalSpace");
        std::map<std::string, double> to_return;
        for (const auto & service : this->storage_services) {
            auto service_name = service->getName();
            auto service_capacity = service->getTotalSpace();
            to_return[service_name] = std::accumulate(service_capacity.begin(), service_capacity.end(), 0, 
                                                        [](const std::size_t previous, const auto& element){ return previous + element.second;});
        }
        return to_return;
    }

    /**
     * @brief Synchronously asks the storage services inside the compound storage service 
     *        for their free space at all of their mount points
     * 
     * @return The free space in bytes of each mount point, as a map
     *
     * @throw ExecutionException
     *
     * @throw std::runtime_error
     *
     */
    std::map<std::string, double> CompoundStorageService::getFreeSpace() {

        WRENCH_INFO("CompoundStorageService::getFreeSpace Forwarding request to internal services");

        std::map<std::string, double> to_return = {}; 
        std::map<std::string, simgrid::s4u::Mailbox*> mailboxes = {};

        for (const auto & service : this->storage_services) {
            
            auto free_space = service->getFreeSpace();

            to_return[service->getName()] = std::accumulate(free_space.begin(), free_space.end(), 0, 
                                                          [](const std::size_t previous, const auto& element){return previous + element.second;}
            );

        }

        return to_return;
    }

    void CompoundStorageService::setScratch() {
        WRENCH_WARN("CompoundStorageService::setScratch Forbidden");
        throw std::logic_error("CompoundStorageService can't be setup as a scratch space, it is only an abstraction layer.");
    }

    /**
     * @brief Return the set of all services accessible through this CompoundStorageService
     * 
     * @return The set of known StorageServices.
    */
    std::set<std::shared_ptr<StorageService>>& CompoundStorageService::getAllServices() {
        WRENCH_INFO("CompoundStorageService::getAllServices");
        return this->storage_services;
    }


    /**
     * @brief Helper method to validate property values
     * throw std::invalid_argument
     */
    void CompoundStorageService::validateProperties() {
        auto value = this->getPropertyValueAsString(CompoundStorageServiceProperty::STORAGE_SELECTION_METHOD);
        if (value != "external") {
            WRENCH_INFO("CompoundStorageService::validateProperties Incorrect property for STORAGE_SELECTION_METHOD");
            throw std::invalid_argument("CompoundStorageService::validateProperties Only 'external' storage selection method is currently allowed");
        }
    }
    /**
     * @brief Get a file's last write date at a location (in zero simulated time)
     *
     * @param location: the file location
     *
     * @return the file's last write date, or -1 if the file is not found
     *
     */
    double CompoundStorageService::getFileLastWriteDate(const std::shared_ptr<FileLocation> &location) {
        WRENCH_INFO("CompoundStorageService::getFileLastWriteDate Not implemented, call internal services instead");
        throw std::logic_error("CompoundStorageService::getFileLastWriteDate(): CompoundStorageService"
                                 " doesn't have a LogicalFileSystem. Call on internal storage service(s) instead");
    }

    /**
    * @brief Generate a unique number
    *
    * @return a unique number
    */
    unsigned long CompoundStorageService::getNewUniqueNumber() {
        static unsigned long sequence_number = 0;
        return (sequence_number++);
    }

    /**
     * @brief Process a stop daemon request
     * @param ack_mailbox: the mailbox to which the ack should be sent
     * @return false if the daemon should terminate
     */
    bool CompoundStorageService::processStopDaemonRequest(simgrid::s4u::Mailbox *ack_mailbox) {
        try {
            S4U_Mailbox::putMessage(ack_mailbox,
                                    new ServiceDaemonStoppedMessage(this->getMessagePayloadValue(
                                            CompoundStorageServiceMessagePayload::DAEMON_STOPPED_MESSAGE_PAYLOAD)));
        } catch (ExecutionException &e) {
            return false;
        }
        return false;
    }

};// namespace wrench