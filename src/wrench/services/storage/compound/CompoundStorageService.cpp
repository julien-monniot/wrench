#include <wrench/services/storage/compound/CompoundStorageService.h>
#include <wrench/services/ServiceMessage.h>
#include "wrench/services/storage/StorageServiceMessage.h"
#include <wrench/services/storage/StorageServiceMessagePayload.h>
#include <wrench/simgrid_S4U_util/S4U_Mailbox.h>
#include <wrench/failure_causes/FileNotFound.h>
#include <wrench/exceptions/ExecutionException.h>
#include <wrench/logging/TerminalOutput.h>

WRENCH_LOG_CATEGORY(wrench_core_compound_storage_system,
                    "Log category for Compound Storage Service");

namespace wrench { 

    CompoundStorageService::CompoundStorageService(const std::string &hostname,
                                                   std::set<std::shared_ptr<StorageService>> storage_services,
                                                   WRENCH_PROPERTY_COLLECTION_TYPE property_list,
                                                   WRENCH_MESSAGE_PAYLOADCOLLECTION_TYPE messagepayload_list) :
            CompoundStorageService(hostname, storage_services, property_list, messagepayload_list, 
                                   "_" + std::to_string(getNewUniqueNumber())) {};


    CompoundStorageService::CompoundStorageService(
                const std::string &hostname, 
                std::set<std::shared_ptr<StorageService>> storage_services,
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

            // CSS should be non-bufferized, as it actually doesn't copy / transfer anything
            // and this allows it to receive message requests for copy (otherwise, src storage service might receive it)
            this->buffer_size = 0;
            this->storage_services = storage_services;
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

    std::shared_ptr<FileLocation> CompoundStorageService::lookupFileLocation(const std::shared_ptr<FileLocation> &location) {

        WRENCH_INFO("lookupFileLocation:: For file %s", location->getFile()->getID().c_str());

        if (this->file_location_mapping.find(location->getFile()) == this->file_location_mapping.end()) {
            WRENCH_INFO("lookupFileLocation:: file %s NOT found", location->getFile()->getID().c_str());
            return nullptr;
        } else {
            WRENCH_INFO("lookupFileLocation:: file %s found, on storage service %s", 
                        location->getFile()->getID().c_str(), 
                        location->getStorageService()->getName().c_str()
            );
            return this->file_location_mapping[location->getFile()];
        }

    }

    void CompoundStorageService::registerFileLocation(const std::shared_ptr<FileLocation> location, const std::shared_ptr<StorageService> ss, const std::string& absolute_path) {

        WRENCH_INFO("registerFileLocation:: For file %s on storage service %s, at path %s", 
                    location->getFile()->getID().c_str(),
                    ss->getName().c_str(),
                    absolute_path.c_str()            
        );
        // Supposing (and it better be true) that DataFiles are unique throught a given simulation run, even among various jobs.
        this->file_location_mapping[location->getFile()] = FileLocation::LOCATION(ss, absolute_path, location->getFile());

    }

    std::shared_ptr<FileLocation> CompoundStorageService::lookupOrDesignateStorageService(const std::shared_ptr<FileLocation> location) {

        auto concrete_file_location = this->lookupFileLocation(location);

        if (concrete_file_location) {
            WRENCH_INFO("lookupOrDesignateStorageService:: File %s already known by CSS", location->getFile()->getID().c_str());
            return concrete_file_location;
        }

        WRENCH_INFO("lookupOrDesignateStorageService:: File %s not already known by CSS", location->getFile()->getID().c_str());
        auto designatedLocation = designateStorageService(location, this->storage_services);
        WRENCH_INFO("lookupOrDesignateStorageService:: Registering file %s on storage service %s, at path %s", 
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

        return designatedLocation;

    }

    std::shared_ptr<FileLocation> designateStorageService(const std::shared_ptr<FileLocation> location, const std::set<std::shared_ptr<StorageService>> resources) {
        
        auto capacity_req = location->getFile()->getSize();
        
        std::shared_ptr<FileLocation> designated_location = nullptr;

        for(const auto& storage_service : resources) {

            auto free_space = storage_service->getFreeSpace();
            for (const auto& free_space_entry : free_space) {
                if (free_space_entry.second >= capacity_req) {
                    designated_location = FileLocation::LOCATION(storage_service, free_space_entry.first, location->getFile());
                    break;
                }
            }
        }

        return designated_location;

    }

    bool CompoundStorageService::processFileDeleteRequest(StorageServiceFileDeleteRequestMessage *msg) {

        auto designated_location = this->lookupFileLocation(msg->location);
        if (!designated_location) {
            
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
                /*const std::shared_ptr<FileLocation> &src,
                const std::shared_ptr<FileLocation> &dst,
                simgrid::s4u::Mailbox *answer_mailbox) {*/

        // Check that source file location DOES NOT reference a CSS. If so, abort.
        if (std::dynamic_pointer_cast<CompoundStorageService>(msg->src->getStorageService())) {
           
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

        // Lookup  dest file locally, or find suitable storage service to host it.
        auto designated_location = this->lookupOrDesignateStorageService(msg->dst);
        if (!designated_location) {
            // abort and send answer directly from CSS
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
            designated_location->getStorageService()->mailbox,
            new StorageServiceFileCopyRequestMessage(
                    msg->answer_mailbox,
                    msg->src,
                    FileLocation::LOCATION(
                        designated_location->getStorageService(), 
                        designated_location->getFullAbsolutePath(), 
                        designated_location->getFile()
                    ),
                    nullptr,
                    designated_location->getStorageService()->getMessagePayloadValue(
                            StorageServiceMessagePayload::FILE_COPY_REQUEST_MESSAGE_PAYLOAD)
            )
        );

        return true;
    }

    bool CompoundStorageService::processFileWriteRequest(StorageServiceFileWriteRequestMessage* msg) {

        auto designated_location = this->lookupOrDesignateStorageService(msg->location);
        if (!designated_location) {
            // abort and send answer directly from CSS
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
                wrench::FileLocation::LOCATION(
                    designated_location->getStorageService(), 
                    designated_location->getFullAbsolutePath(), 
                    designated_location->getFile()
                ),
                msg->buffer_size,
                this->getMessagePayloadValue(
                    StorageServiceMessagePayload::FILE_WRITE_REQUEST_MESSAGE_PAYLOAD)
            )
        );

        return true;
    }

    bool CompoundStorageService::processFileReadRequest(StorageServiceFileReadRequestMessage* msg) {        
        
        auto designated_location = this->lookupFileLocation(msg->location);
        if (!designated_location) {

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

        // The file is known, we can forward the request to the underlying designated StorageService
        S4U_Mailbox::putMessage(
            designated_location->getStorageService()->mailbox,
            new StorageServiceFileReadRequestMessage(
                msg->answer_mailbox,
                msg->requesting_host,
                msg->mailbox_to_receive_the_file_content,
                FileLocation::LOCATION(
                    designated_location->getStorageService(),
                    designated_location->getFullAbsolutePath(), 
                    designated_location->getFile()
                ),
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