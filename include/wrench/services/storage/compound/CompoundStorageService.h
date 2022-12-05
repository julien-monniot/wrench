/**
 * Copyright (c) 2017-2020. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef WRENCH_COMPOUNDSTORAGESERVICE_H
#define WRENCH_COMPOUNDSTORAGESERVICE_H

#include "wrench/services/storage/StorageService.h"
#include "wrench/services/memory/MemoryManager.h"
#include "wrench/simgrid_S4U_util/S4U_PendingCommunication.h"
#include "wrench/services/storage/compound/CompoundStorageServiceProperty.h"
#include "wrench/services/storage/compound/CompoundStorageServiceMessagePayload.h"

namespace wrench {

    //class SimulationMessage;

    //class SimulationTimestampFileCopyStart;

    //class S4U_PendingCommunication;

    /**
     * @brief A storage service that provides an abstraction over multiple other concrete storage services
     *        Instead of giving direct access to some storage resources, it grants access to a set of 
     *        concrete storage services which in turn provide the actual storage resources (one or more disks 
     *        each). This allows a user to choose that a file produced during the execution of a job/workflow
     *        will be placed on one of these storage services, whithout manually choosing where. However, 
     *        this requires that the resource manager (BatchComputeManager typically) is able to do make this 
     *        choice.
     */
    class CompoundStorageService : public StorageService {
    
    public:

        //~CompoundStorageService() override;

        CompoundStorageService(const std::string &hostname,
                               std::set<std::shared_ptr<StorageService>> storage_services,
                               WRENCH_PROPERTY_COLLECTION_TYPE property_list = {},
                               WRENCH_MESSAGE_PAYLOADCOLLECTION_TYPE messagepayload_list = {}); 


        /*  Mandatory override of virtual function in StorageService, even though we might not actually
            implement it for real ?
        */
        double getFileLastWriteDate(const std::shared_ptr<FileLocation> &location) override;

        // Mandatory override from virtual function in StorageService
        double getLoad() override;

    protected:
        CompoundStorageService(const std::string &hostname, 
                               std::set<std::shared_ptr<StorageService>> storage_services,
                               WRENCH_PROPERTY_COLLECTION_TYPE property_list,
                               WRENCH_MESSAGE_PAYLOADCOLLECTION_TYPE messagepayload_list,
                               const std::string &suffix);

        WRENCH_PROPERTY_COLLECTION_TYPE default_property_values = {
                {CompoundStorageServiceProperty::STORAGE_SELECTION_METHOD, "external"},
        };

        /** @brief Default message payload values */
        WRENCH_MESSAGE_PAYLOADCOLLECTION_TYPE default_messagepayload_values = {
                {CompoundStorageServiceMessagePayload::STOP_DAEMON_MESSAGE_PAYLOAD, 1024},
                {CompoundStorageServiceMessagePayload::DAEMON_STOPPED_MESSAGE_PAYLOAD, 1024},
        };

        static unsigned long getNewUniqueNumber();

        bool processStopDaemonRequest(simgrid::s4u::Mailbox *ack_mailbox);

    private:
        friend class Simulation;

        int main() override;
        
        std::set<std::shared_ptr<StorageService>> storage_services = {};
    };

};// namespace wrench

#endif //WRENCH_COMPOUNDSTORAGESERVICE_H
