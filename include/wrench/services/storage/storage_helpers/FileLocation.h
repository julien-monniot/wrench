/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef WRENCH_FILELOCATION_H
#define WRENCH_FILELOCATION_H

#include <memory>


namespace wrench {

    /***********************/
    /** \cond DEVELOPER    */
    /***********************/

    class StorageService;

    class FileLocation {

    public:

        static std::shared_ptr<FileLocation> LOCATION(std::shared_ptr<StorageService> ss);

        static std::shared_ptr<FileLocation> LOCATION(std::shared_ptr<StorageService> ss,
                                                      std::string absolute_path);

        std::shared_ptr<StorageService> getStorageService();
        std::string getMountPoint();
        std::string getAbsolutePathAtMountPoint();

        std::string toString();


        bool operator==(const std::shared_ptr<FileLocation> &rhs) {
            return ((this->getStorageService() == rhs->getStorageService()) and
            (this->getAbsolutePathAtMountPoint() == rhs->getAbsolutePathAtMountPoint()));
        }

        /**
         * @brief Overloaded "not equal" operator
         * @param rhs: right-hand side of the comparison
         * @return true if different, false otherwise
         */
        bool operator!=(const std::shared_ptr<FileLocation> &rhs) {
            return not FileLocation::operator==(rhs);
        }


    private:

        friend class LogicalFileSystem;

        /**
         * @brief Constructor
         * @param ss: the storage service
         * @param ap: the absolute path
         */
        FileLocation(std::shared_ptr<StorageService> ss, std::string mp, std::string apamp) :
                storage_service(ss), mount_point(mp), absolute_path_at_mount_point(apamp) { }

        std::shared_ptr<StorageService> storage_service;
        std::string mount_point;
        std::string absolute_path_at_mount_point;

        static std::string sanitizePath(std::string path);

    };


    /***********************/
    /** \endcond           */
    /***********************/


}


#endif //WRENCH_FILELOCATION_H
