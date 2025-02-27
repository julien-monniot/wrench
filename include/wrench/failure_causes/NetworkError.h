/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef WRENCH_NETWORK_ERROR_H
#define WRENCH_NETWORK_ERROR_H

#include <set>
#include <string>

#include "FailureCause.h"

namespace wrench {


    /***********************/
    /** \cond DEVELOPER    */
    /***********************/

    /**
     * @brief A "network error (or endpoint is down)" failure cause
     */
    class NetworkError : public FailureCause {
    public:
        /** @brief Enumerated type to describe whether the network error occured
         * while sending or receiving
         */
        enum OperationType {
            SENDING,
            RECEIVING
        };

        /** @brief Enumerated type to describe the type of the network error
         */
        enum ErrorType {
            TIMEOUT,
            FAILURE
        };

        /***********************/
        /** \cond INTERNAL     */
        /***********************/
        NetworkError(NetworkError::OperationType, NetworkError::ErrorType, const std::string &mailbox);
        /***********************/
        /** \endcond           */
        /***********************/

        std::string toString() override;
        bool whileReceiving();
        bool whileSending();
        bool isTimeout();
        std::string getMailbox();

    private:
        NetworkError::OperationType operation_type;
        NetworkError::ErrorType error_type;
        std::string mailbox = "";
    };


    /***********************/
    /** \endcond           */
    /***********************/
}// namespace wrench


#endif//WRENCH_NETWORK_ERROR_H
