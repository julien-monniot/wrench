/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#ifndef WRENCH_COMPOUNDSTORAGESERVICEPROPERTY_H
#define WRENCH_COMPOUNDSTORAGESERVICEPROPERTY_H

#include "wrench/services/storage/StorageServiceProperty.h"

namespace wrench {

    /**
    * @brief Configurable properties for a CompoundStorageService
    */
    class CompoundStorageServiceProperty : public StorageServiceProperty {

    public:
        /** @brief Method for selecting concrete storage for each file submitted to the CompoundStorageService **/
        DECLARE_PROPERTY_NAME(STORAGE_SELECTION_METHOD);
    };

};// namespace wrench


#endif//WRENCH_COMPOUNDSTORAGESERVICEPROPERTY_H