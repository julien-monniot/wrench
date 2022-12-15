/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include <wrench-dev.h>
#include <wrench/services/storage/storage_helpers/LogicalFileSystem.h>

#include <limits>
#include <utility>

WRENCH_LOG_CATEGORY(wrench_core_logical_file_system, "Log category for Logical File System");


namespace wrench {
    const std::string LogicalFileSystem::DEV_NULL = "/dev/null";
    std::map<std::string, StorageService *> LogicalFileSystem::mount_points;

    /**
     * @brief Constructor
     * @param hostname: the host on which the file system is located
     * @param storage_service: the storage service this file system is for
     * @param mount_point: the mount point, defaults to /dev/null
     * @param eviction_policy: "NONE" or "LRU"
     */
    LogicalFileSystem::LogicalFileSystem(const std::string &hostname, StorageService *storage_service,
                                         std::string mount_point, std::string eviction_policy) {
        if (storage_service == nullptr) {
            throw std::invalid_argument("LogicalFileSystem::LogicalFileSystem(): nullptr storage_service argument");
        }
        this->hostname = hostname;
        this->storage_service = storage_service;
        this->content["/"] = {};

        this->initialized = false;
        if (mount_point == DEV_NULL) {
            devnull = true;
            this->total_capacity = std::numeric_limits<double>::infinity();
        } else {
            devnull = false;
            mount_point = FileLocation::sanitizePath("/" + mount_point + "/");
            // Check validity
            this->disk = S4U_Simulation::hostHasMountPoint(hostname, mount_point);
            if (not this->disk) {
                throw std::invalid_argument("LogicalFileSystem::LogicalFileSystem(): Host " +
                                            hostname + " does not have a disk mounted at " + mount_point);
            }
            this->total_capacity = S4U_Simulation::getDiskCapacity(hostname, mount_point);
        }
        this->free_space = this->total_capacity;
        this->mount_point = mount_point;
        this->eviction_policy = std::move(eviction_policy);
        this->uses_lru = (this->eviction_policy == "LRU");
    }


    /**
     * @brief Initializes the Logical File System (must be called before any other operation on this file system)
     */
    void LogicalFileSystem::init() {
        // Check uniqueness
        auto lfs = LogicalFileSystem::mount_points.find(this->hostname + ":" + this->mount_point);

        if (lfs != LogicalFileSystem::mount_points.end() && mount_point != DEV_NULL) {
            if (lfs->second != this->storage_service) {
                throw std::invalid_argument("LogicalFileSystem::init(): A FileSystem with mount point " + this->mount_point + " at host " + this->hostname + " already exists");
            } else {
                return;
            }
        } else {
            LogicalFileSystem::mount_points[this->hostname + ":" + this->mount_point] = this->storage_service;
            this->initialized = true;
        }
    }

    /**
 * @brief Create a new directory
 *
 * @param absolute_path: the directory's absolute path
 * @throw std::invalid_argument
 */
    void LogicalFileSystem::createDirectory(const std::string &absolute_path) {
        if (devnull) {
            return;
        }
        assertInitHasBeenCalled();
        assertDirectoryDoesNotExist(absolute_path);
        this->content[absolute_path] = {};
    }

    /**
 * @brief Checks whether a directory exists
 * @param absolute_path the directory's absolute path
 * @return true if the directory exists
 */
    bool LogicalFileSystem::doesDirectoryExist(const std::string &absolute_path) {
        if (devnull) {
            return false;
        }
        assertInitHasBeenCalled();
        return (this->content.find(absolute_path) != this->content.end());
    }

    /**
 * @brief Checks whether a directory is empty
 * @param absolute_path: the directory's absolute path
 * @return true if the directory is empty
 *
 * @throw std::invalid_argument
 */
    bool LogicalFileSystem::isDirectoryEmpty(const std::string &absolute_path) {
        if (devnull) {
            return true;
        }
        assertInitHasBeenCalled();
        assertDirectoryExist(absolute_path);
        return (this->content[absolute_path].empty());
    }

    /**
 * @brief Remove an empty directory
 * @param absolute_path: the directory's absolute path
 *
 * @throw std::invalid_argument
 */
    void LogicalFileSystem::removeEmptyDirectory(const std::string &absolute_path) {
        if (devnull) {
            return;
        }
        assertInitHasBeenCalled();
        assertDirectoryExist(absolute_path);
        assertDirectoryIsEmpty(absolute_path);
        this->content.erase(absolute_path);
    }

/**
 * @brief Store file in directory
 *
 * @param file: the file to store
 * @param absolute_path: the directory's absolute path (at the mount point)
 *
 * @throw std::invalid_argument
 */
    void LogicalFileSystem::storeFileInDirectory(const std::shared_ptr<DataFile> &file, const std::string &absolute_path) {
        if (devnull) {
            return;
        }
        assertInitHasBeenCalled();
        // If directory does not exit, create it
        if (not doesDirectoryExist(absolute_path)) {
            createDirectory(absolute_path);
        }

        this->content[absolute_path][file] = std::make_tuple(S4U_Simulation::getClock(), this->last_accessed++);
        this->lru_list.insert(std::make_pair(this->last_accessed - 1, std::make_tuple(absolute_path, file)));

        std::string key = FileLocation::sanitizePath(absolute_path) + file->getID();
        if (this->reserved_space.find(key) == this->reserved_space.end()) {
            this->free_space -= file->getSize();
        } else {
            this->reserved_space.erase(key);
        }
    }

    /**
 * @brief Remove a file in a directory
 * @param file: the file to remove
 * @param absolute_path: the directory's absolute path
 *
 * @throw std::invalid_argument
 */
    void LogicalFileSystem::removeFileFromDirectory(const std::shared_ptr<DataFile> &file, const std::string &absolute_path) {
        if (devnull) {
            return;
        }
        assertInitHasBeenCalled();
        assertDirectoryExist(absolute_path);
        assertFileIsInDirectory(file, absolute_path);
        auto seq = std::get<1>(this->content[absolute_path][file]);
        this->content[absolute_path].erase(file);
        this->lru_list.erase(seq);
        this->free_space += file->getSize();
    }

    /**
 * @brief Remove all files in a directory
 * @param absolute_path: the directory's absolute path
 *
 * @throw std::invalid_argument
 */
    void LogicalFileSystem::removeAllFilesInDirectory(const std::string &absolute_path) {
        if (devnull) {
            return;
        }
        assertInitHasBeenCalled();
        assertDirectoryExist(absolute_path);
        for (auto const &c : this->content[absolute_path]) {
            this->lru_list.erase(std::get<1>(c.second));
        }
        this->content[absolute_path].clear();
    }

    /**
 * @brief Checks whether a file is in a directory
 * @param file: the file
 * @param absolute_path: the directory's absolute path
 *
 * @return true if the file is present
 *
 * @throw std::invalid_argument
 */
    bool LogicalFileSystem::isFileInDirectory(const std::shared_ptr<DataFile> &file, const std::string &absolute_path) {
        if (devnull) {
            return false;
        }
        assertInitHasBeenCalled();
        // If directory does not exist, say "no"
        if (not doesDirectoryExist(absolute_path)) {
            return false;
        }

        return (this->content[absolute_path].find(file) != this->content[absolute_path].end());
    }

    /**
 * @brief Get the files in a directory as a set
 * @param absolute_path: the directory's absolute path
 *
 * @return a set of files
 *
 * @throw std::invalid_argument
 */
    std::set<std::shared_ptr<DataFile>> LogicalFileSystem::listFilesInDirectory(const std::string &absolute_path) {
        std::set<std::shared_ptr<DataFile>> to_return;
        if (devnull) {
            return {};
        }
        assertInitHasBeenCalled();
        assertDirectoryExist(absolute_path);
        for (auto const &f: this->content[absolute_path]) {
            to_return.insert(f.first);
        }
        return to_return;
    }

    /**
 * @brief Get the total capacity
 * @return the total capacity
 */
    double LogicalFileSystem::getTotalCapacity() {
        assertInitHasBeenCalled();
        return this->total_capacity;
    }

    /**
 * @brief Checks whether there is enough space to store some number of bytes
 * @param bytes: a number of bytes
 * @return true if the number of bytes can fit
 */
    bool LogicalFileSystem::hasEnoughFreeSpace(double bytes) {
        assertInitHasBeenCalled();
        return (this->free_space >= bytes);
    }

    /**
 * @brief Get the file system's free space
 * @return the free space in bytes
 */
    double LogicalFileSystem::getFreeSpace() {
        assertInitHasBeenCalled();
        return this->free_space;
    }

    /**
    * @brief Reserve space for a file that will be stored
    * @param file: the file
    * @param absolute_path: the path where it will be written
    *
    * @return true on success, false on failure
    * @throw std::invalid_argument
    */
    bool LogicalFileSystem::reserveSpace(const std::shared_ptr<DataFile> &file,
                                         const std::string &absolute_path) {
        if (devnull) {
            return true;
        }

        assertInitHasBeenCalled();

        std::string key = FileLocation::sanitizePath(absolute_path) + file->getID();
        if (this->reserved_space.find(key) != this->reserved_space.end()) {
            WRENCH_WARN("LogicalFileSystem::reserveSpace(): Space was already being reserved for storing file %s at path %s:%s. "
                        "This is likely a redundant copy, and nothing needs to be done",
                        file->getID().c_str(), this->hostname.c_str(), absolute_path.c_str());
        }

        if (this->free_space < file->getSize()) {

            if (this->eviction_policy == "NONE") {
                return false;

            } else if (this->eviction_policy == "LRU") {
                if (!evictLRUFiles(file->getSize())) {
                    return false;
                }

            } else {
                throw std::invalid_argument("LogicalFileSystem::reserveSpace(): Invalid eviction policy " + eviction_policy);
            }
        }

        this->reserved_space[key] = file->getSize();
        this->free_space -= file->getSize();
        return false;
    }

    /**
     * @brief Unreserved space that was saved for a file (likely a failed transfer)
     * @param file: the file
     * @param absolute_path: the path where it would have been written
     * @throw std::invalid_argument
     */
    void LogicalFileSystem::unreserveSpace(const std::shared_ptr<DataFile> &file, std::string absolute_path) {
        if (devnull) {
            return;
        }
        assertInitHasBeenCalled();
        std::string key = FileLocation::sanitizePath(std::move(absolute_path)) + file->getID();

        if (this->reserved_space.find(key) == this->reserved_space.end()) {
            return;// oh well, the transfer was cancelled/terminated/whatever
        }

        //         This will never happen
        //        if (this->occupied_space <  file->getSize()) {
        //            throw std::invalid_argument("LogicalFileSystem::unreserveSpace(): Occupied space is less than the file size... should not happen");
        //        }

        this->reserved_space.erase(key);
        this->free_space += file->getSize();
    }


    /**
     * @brief Stage file in directory
     * @param file: the file to stage
     * @param absolute_path: the directory's absolute path (at the mount point)
     *
     * @throw std::invalid_argument
     */
    void LogicalFileSystem::stageFile(const std::shared_ptr<DataFile> &file, std::string absolute_path) {
        if (devnull) {
            return;
        }
        // If Space is not sufficient, forget it
        if (this->free_space < file->getSize()) {
            throw std::invalid_argument("LogicalFileSystem::stageFile(): Insufficient space to store file " +
                                        file->getID() + " at " + this->hostname + ":" + absolute_path);
        }

        absolute_path = wrench::FileLocation::sanitizePath(absolute_path);

        // If directory does not exit, create it
        if (this->content.find(absolute_path) == this->content.end()) {
            this->content[absolute_path] = {};
        }

        // If file does  not already exist, create it
        if (this->content.find(absolute_path) != this->content.end()) {
            this->content[absolute_path][file] = std::make_tuple(S4U_Simulation::getClock(), this->last_accessed++);
            this->lru_list.erase(this->last_accessed - 1);
            this->free_space -= file->getSize();
        } else {
            return;
        }
    }

    /**
     * @brief Retrieve the file's last write date
     * @param file: the file
     * @param absolute_path: the file path
     * @return a date in seconds (returns -1.0) if file in not found
     */
    double LogicalFileSystem::getFileLastWriteDate(const shared_ptr<DataFile> &file, const string &absolute_path) {

        if (devnull) {
            return -1;
        }
        assertInitHasBeenCalled();
        // If directory does not exist, say "no"
        if (not doesDirectoryExist(absolute_path)) {
            return -1;
        }

        if (this->content[absolute_path].find(file) != this->content[absolute_path].end()) {
            return std::get<0>(this->content[absolute_path][file]);
        } else {
            return -1;
        }
    }

    /**
     * @brief Update a file's read date
     * @param file: the file
     * @param absolute_path: the path
     */
    void LogicalFileSystem::updateReadDate(const std::shared_ptr<DataFile> &file, const std::string &absolute_path) {
        if (devnull) {
            return;
        }
        assertInitHasBeenCalled();
        // If directory does not exist, do nothing
        if (not doesDirectoryExist(absolute_path)) {
            return;
        }

        if (this->content[absolute_path].find(file) != this->content[absolute_path].end()) {
            std::get<1>(this->content[absolute_path][file]) = this->last_accessed++;
        }
    }


    /**
     * @brief Get the disk on which this file system runs
     * @return The SimGrid disk on which this file system is mounted
     */
    simgrid::s4u::Disk *LogicalFileSystem::getDisk() {
        return this->disk;
    }

    /**
     * @brief Evict LRU files to create some free space
     * @param needed_free_space: the needed free space
     * @return true on success, false on failure
     */
    bool LogicalFileSystem::evictLRUFiles(double needed_free_space) {

        double total_reserved_space = 0;
        for (auto const &x : this->reserved_space) {
            total_reserved_space += x.second;
        }

        if (this->total_capacity - total_reserved_space < needed_free_space) return false;

        while(this->free_space < needed_free_space) {
            unsigned int key = this->lru_list.begin()->first;
            auto path = std::get<0>(this->lru_list.begin()->second);
            auto file = std::get<1>(this->lru_list.begin()->second);
            this->lru_list.erase(key);
            this->content[path].erase(file);
            this->free_space += file->getSize();
        }
        return true;

    }

}// namespace wrench
