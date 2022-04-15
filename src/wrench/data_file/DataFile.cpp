/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include <map>
#include <utility>
#include <wrench/logging/TerminalOutput.h>

#include <wrench/data_file/DataFile.h>
#include <wrench/workflow/WorkflowTask.h>

WRENCH_LOG_CATEGORY(wrench_core_data_file, "Log category for DataFile");

namespace wrench {

    /**
     * @brief Constructor
     *
     * @param id: the file id
     * @param s: the file size
     */
    DataFile::DataFile(std::string id, double s) : id(std::move(id)), size(s) {
    }

    /**
     * @brief Get the file size
     * @return a size in bytes
     */
    double DataFile::getSize() const {
        return this->size;
    }

    /**
     * @brief Get the file id
     * @return the id
     */
    std::string DataFile::getID() const {
        return this->id;
    }

}// namespace wrench
