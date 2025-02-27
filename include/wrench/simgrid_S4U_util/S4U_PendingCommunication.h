/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#ifndef WRENCH_S4U_PENDINGCOMMUNICATION_H
#define WRENCH_S4U_PENDINGCOMMUNICATION_H


#include <vector>
#include <simgrid/s4u/Comm.hpp>
#include "wrench/util/MessageManager.h"
#include "wrench/simulation/SimulationMessage.h"

namespace wrench {

    //    class SimulationMessage;

    /*******************/
    /** \cond INTERNAL */
    /*******************/

    /** @brief This is a simple wrapper class around S4U asynchronous communication checking methods */
    class S4U_PendingCommunication {
    public:
        /**
         * @brief The communication operation's type
         */
        enum OperationType {
            SENDING,
            RECEIVING
        };

        /**
         * @brief Constructor
         *
         * @param mailbox: the mailbox
         * @param operation_type: the operation type
         */
        S4U_PendingCommunication(simgrid::s4u::Mailbox *mailbox, OperationType operation_type) : mailbox(mailbox), operation_type(operation_type) {}

        std::unique_ptr<SimulationMessage> wait();
        std::unique_ptr<SimulationMessage> wait(double timeout);

        static unsigned long waitForSomethingToHappen(
                const std::vector<std::shared_ptr<S4U_PendingCommunication>> &pending_comms,
                double timeout);

        static unsigned long waitForSomethingToHappen(
                std::vector<S4U_PendingCommunication *> pending_comms,
                double timeout);

        //        ~S4U_PendingCommunication() default;

        /** @brief The SimGrid communication handle */
        simgrid::s4u::CommPtr comm_ptr;
        /** @brief The message */
        std::unique_ptr<SimulationMessage> simulation_message;
        /** @brief The mailbox */
        simgrid::s4u::Mailbox *mailbox;
        /** @brief The operation type */
        OperationType operation_type;
    };

    /*******************/
    /** \endcond */
    /*******************/

}// namespace wrench


#endif//WRENCH_S4U_PENDINGCOMMUNICATION_H
