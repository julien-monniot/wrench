#include <wrench/services/storage/compound/CompoundStorageService.h>
#include <wrench/services/ServiceMessage.h>
#include "wrench/services/storage/StorageServiceMessage.h"
#include <wrench/services/storage/StorageServiceMessagePayload.h>
#include <wrench/simgrid_S4U_util/S4U_Mailbox.h>
#include <wrench/failure_causes/FunctionalityNotAvailable.h>
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
                    "compound_storage" + suffix)
        {

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
            /* // We don't have running transactions in this service, other than 
               // comms
            for (auto const &transaction: this->running_transactions) {
                pending_activities.emplace_back(transaction->stream);
            }
            */

            // Wait one activity (communication in this case) to complete
            int finished_activity_index;
            try {
                finished_activity_index = (int) simgrid::s4u::Activity::wait_any(pending_activities);
            } catch (simgrid::NetworkFailureException &e) {
                // the comm failed
                comm_ptr_has_been_posted = false;
                continue;// oh well
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
            return processFileDeleteRequest(msg->location, msg->answer_mailbox);

        } else if (auto msg = dynamic_cast<StorageServiceFileLookupRequestMessage *>(message)) {
            return processFileLookupRequest(msg->location, msg->answer_mailbox);

        } else if (auto msg = dynamic_cast<StorageServiceFileWriteRequestMessage *>(message)) {
            return processFileWriteRequest(msg->location, msg->answer_mailbox,
                                           msg->requesting_host, msg->buffer_size);

        } else if (auto msg = dynamic_cast<StorageServiceFileReadRequestMessage *>(message)) {
            return processFileReadRequest(msg->location,
                                          msg->num_bytes_to_read, msg->answer_mailbox, msg->requesting_host);

        } else if (auto msg = dynamic_cast<StorageServiceFileCopyRequestMessage *>(message)) {
            return processFileCopyRequest(msg->src, msg->dst, msg->answer_mailbox);

        } else {
            throw std::runtime_error(
                    "CompoundStorageService::processNextMessage(): Unexpected [" + message->getName() + "] message." + 
                    "This is only an abstraction layer and it can't be used as an actual storage service");
        }
    }

    bool CompoundStorageService::processFileDeleteRequest(const std::shared_ptr<FileLocation> &location, simgrid::s4u::Mailbox *answer_mailbox) {

        try {        
            S4U_Mailbox::dputMessage(
                answer_mailbox,
                new StorageServiceFileDeleteAnswerMessage(
                        nullptr,
                        this->getSharedPtr<CompoundStorageService>(),
                        false,
                        std::shared_ptr<FailureCause>(new FunctionalityNotAvailable(
                            this->getSharedPtr<CompoundStorageService>(), "FileDelete")
                        ),
                        this->getMessagePayloadValue(
                                CompoundStorageServiceMessagePayload::FILE_DELETE_ANSWER_MESSAGE_PAYLOAD)));
        } catch (wrench::ExecutionException &e) {}

        return true;
    }
    
    bool CompoundStorageService::processFileLookupRequest(const std::shared_ptr<FileLocation> &location, simgrid::s4u::Mailbox *answer_mailbox) {
        
        try {   
            S4U_Mailbox::dputMessage(
                answer_mailbox,
                new StorageServiceFileLookupAnswerMessage(
                        nullptr, false,
                        this->getMessagePayloadValue(
                            CompoundStorageServiceMessagePayload::FILE_LOOKUP_ANSWER_MESSAGE_PAYLOAD)));
        } catch (wrench::ExecutionException &e) {}

        return true;
    }

    bool CompoundStorageService::processFileCopyRequest(
                const std::shared_ptr<FileLocation> &src,
                const std::shared_ptr<FileLocation> &dst,
                simgrid::s4u::Mailbox *answer_mailbox) {

        try {
            S4U_Mailbox::putMessage(
                    answer_mailbox,
                    new StorageServiceFileCopyAnswerMessage(
                            src,
                            dst,
                            nullptr, 
                            false,
                            false,
                            std::shared_ptr<FailureCause>(new FunctionalityNotAvailable(
                                this->getSharedPtr<CompoundStorageService>(), "FileCopy")
                            ),
                            this->getMessagePayloadValue(
                                    CompoundStorageServiceMessagePayload::FILE_COPY_ANSWER_MESSAGE_PAYLOAD))
                    );
        } catch (ExecutionException &e) {}

        return true;
    }

    bool CompoundStorageService::processFileWriteRequest(const std::shared_ptr<FileLocation> &location,
                                     simgrid::s4u::Mailbox *answer_mailbox, simgrid::s4u::Host *requesting_host,
                                     double buffer_size) {

        try {
            S4U_Mailbox::dputMessage(
                answer_mailbox,
                new StorageServiceFileWriteAnswerMessage(
                        location,
                        false,
                        std::shared_ptr<FailureCause>(new FunctionalityNotAvailable(
                            this->getSharedPtr<CompoundStorageService>(), "FileWrite")
                        ),
                        nullptr,
                        this->getMessagePayloadValue(
                                CompoundStorageServiceMessagePayload::FILE_WRITE_ANSWER_MESSAGE_PAYLOAD)));
        } catch (wrench::ExecutionException &e) {}

        return true;
    }

    bool CompoundStorageService::processFileReadRequest(const std::shared_ptr<FileLocation> &location,
                               double num_bytes_to_read, simgrid::s4u::Mailbox *answer_mailbox,
                               simgrid::s4u::Host *requesting_host) {
        
        try {
            S4U_Mailbox::dputMessage(
                    answer_mailbox,
                    new StorageServiceFileReadAnswerMessage(
                            location,
                            false,
                            std::shared_ptr<FailureCause>(
                                new FunctionalityNotAvailable(this->getSharedPtr<CompoundStorageService>(), "FileRead")
                            ),
                            0,
                            this->getMessagePayloadValue(
                                    CompoundStorageServiceMessagePayload::FILE_READ_ANSWER_MESSAGE_PAYLOAD))
            );
        } catch (wrench::ExecutionException &e) {}

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