/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#include <gtest/gtest.h>

#include <wrench-dev.h>
#include "../../../include/TestWithFork.h"
#include "../../../include/UniqueTmpPathPrefix.h"

WRENCH_LOG_CATEGORY(Compound_storage_service_functional_test, "Log category for CompoundStorageServiceFunctionalTest");


class CompoundStorageServiceFunctionalTest : public ::testing::Test {

public:

    std::shared_ptr<wrench::DataFile> file_1;
    std::shared_ptr<wrench::DataFile> file_10;
    std::shared_ptr<wrench::DataFile> file_100;
    std::shared_ptr<wrench::DataFile> file_500;

    std::shared_ptr<wrench::SimpleStorageService> simple_storage_service_100 = nullptr;
    std::shared_ptr<wrench::SimpleStorageService> simple_storage_service_510 = nullptr;
    std::shared_ptr<wrench::SimpleStorageService> simple_storage_service_1000 = nullptr;
    std::shared_ptr<wrench::CompoundStorageService> compound_storage_service = nullptr;

    std::shared_ptr<wrench::ComputeService> compute_service = nullptr;

    void do_BasicFunctionality_test();

protected:
    ~CompoundStorageServiceFunctionalTest() {
    }

    CompoundStorageServiceFunctionalTest() {

        // Create the files
        file_1 = wrench::Simulation::addFile("file_1", 1.0);
        file_10 = wrench::Simulation::addFile("file_10", 10.0);
        file_100 = wrench::Simulation::addFile("file_100", 100.0);
        file_500 = wrench::Simulation::addFile("file_500", 500.0);

        // Create a three-hosts platform file (2 for simple storage, one for Compound Storage)
        std::string xml = "<?xml version='1.0'?>"
                          "<!DOCTYPE platform SYSTEM \"https://simgrid.org/simgrid.dtd\">"
                          "<platform version=\"4.1\"> "
                          "   <zone id=\"AS0\" routing=\"Full\"> "
                          "       <host id=\"ComputeHost\" speed=\"1f\">"
                          "       </host>"
                          "       <host id=\"SimpleStorageHost0\" speed=\"1f\">"
                          "          <disk id=\"large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"100B\"/>"
                          "             <prop id=\"mount\" value=\"/disk100/\"/>"
                          "          </disk>"
                          "          <disk id=\"other_large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"510B\"/>"
                          "             <prop id=\"mount\" value=\"/disk510/\"/>"
                          "          </disk>"
                          "          <disk id=\"other_other_large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"1000B\"/>"
                          "             <prop id=\"mount\" value=\"/disk1000/\"/>"
                          "          </disk>"
                          "       </host>"
                          "       <host id=\"SimpleStorageHost1\" speed=\"1f\">"
                          "          <disk id=\"large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"100B\"/>"
                          "             <prop id=\"mount\" value=\"/disk100/\"/>"
                          "          </disk>"
                          "          <disk id=\"other_large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"510B\"/>"
                          "             <prop id=\"mount\" value=\"/disk510/\"/>"
                          "          </disk>"
                          "          <disk id=\"other_other_large_disk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"1000B\"/>"
                          "             <prop id=\"mount\" value=\"/disk1000/\"/>"
                          "          </disk>"
                          "       </host>"
                          "       <host id=\"CompoundStorageHost\" speed=\"1f\">"
                          "       </host>"
                          "   </zone> "
                          "</platform>";
        FILE *platform_file = fopen(platform_file_path.c_str(), "w");
        fprintf(platform_file, "%s", xml.c_str());
        fclose(platform_file);
    }

    std::string platform_file_path = UNIQUE_TMP_PATH_PREFIX + "platform.xml";
};


/**********************************************************************/
/**  BASIC FUNCTIONALITY SIMULATION TEST                             **/
/**********************************************************************/

class CompoundStorageServiceBasicFunctionalityTestCtrl : public wrench::ExecutionController {

public:
    CompoundStorageServiceBasicFunctionalityTestCtrl(CompoundStorageServiceFunctionalTest *test,
                                                  std::string hostname) : wrench::ExecutionController(hostname, "test"), test(test) {
    }

private:
    CompoundStorageServiceFunctionalTest *test;

    int main() {


        // Retrieve internal SimpleStorageServices
        auto simple_storage_services = test->compound_storage_service->getAllServices();
        if (simple_storage_services.size() != 2) {
                throw std::runtime_error(
                    "There should be two SimpleStorageServices available from the CompoundStorage. Found " + std::to_string(simple_storage_services.size()) + " instead");
        }


    /*  NOT BEHAVING THE WAY I THOUGH IT WOULD...

        // Verify that total space is correct
        auto capacity = test->compound_storage_service->getTotalSpace();
        auto expected_capacity = std::map<std::string, double>({{"SimpleStorageHost0", 100.0}, {"SimpleStorageHost1", 610.0}});
        if (capacity != expected_capacity) {
            throw std::runtime_error("Total Space available to CompoundStorageService is incorrect");
        }
 
    
        // Verify synchronous request for current free space (currently same as capacity, as no file has been placed on internal services)
        auto free_space = test->compound_storage_service->getFreeSpace();
        if (free_space != expected_capacity) {
            throw std::runtime_error("Total free space available to CompoundStorageService is incorrect");
        }

    */

        // Verify that compound storage service mount point is simply DEV_NULL
        auto mount_point = test->compound_storage_service->getMountPoint();
        if (mount_point != wrench::LogicalFileSystem::DEV_NULL + "/") {
            throw std::runtime_error("CompoundStorageService should have only one LogicalFileSystem::DEV_NULL fs.");
        }

        // We don't support getLoad on CompoundStorageService yet
        try {
            test->compound_storage_service->getLoad();
            throw std::runtime_error("CompoundStorageService doesn't have a getLoad() implemented");
        } catch (std::logic_error &e) {
        }

        {
            auto file_1_loc = wrench::FileLocation::LOCATION(test->compound_storage_service, test->file_1);
            try {
                test->compound_storage_service->getFileLastWriteDate(file_1_loc);
              throw std::runtime_error("CompoundStorageService doesn't have a getFileLastWriteDate() implemented");
          } catch (std::logic_error &e) {}
        }

    
        if (test->compound_storage_service->isScratch() == true) {
            throw std::runtime_error("CompoundStorageService should never have isScratch == true");
        }

        try {
            test->compound_storage_service->setScratch();
            throw std::runtime_error("CompoundStorageService can't be setup as a scratch space");
        } catch (std::logic_error &e) {}
   
        
        auto file_1_loc = wrench::FileLocation::LOCATION(test->compound_storage_service, test->file_1);
    
        /*
        
        // Do a bogus lookup
        try {
            wrench::StorageService::lookupFile(wrench::FileLocation::LOCATION(this->test->storage_service_1000, nullptr));
            throw std::runtime_error("Should not be able to lookup a nullptr file!");
        } catch (std::invalid_argument &) {
        }

        // Do a few queries to storage services
        for (const auto &f: {this->test->file_1, this->test->file_10, this->test->file_100, this->test->file_500}) {
            if ((not wrench::StorageService::lookupFile(wrench::FileLocation::LOCATION(this->test->storage_service_1000, f))) ||
                (wrench::StorageService::lookupFile(wrench::FileLocation::LOCATION(this->test->storage_service_100, f))) ||
                (wrench::StorageService::lookupFile(wrench::FileLocation::LOCATION(this->test->storage_service_510, f)))) {
                throw std::runtime_error("Some storage services do/don't have the files that they shouldn't/should have");
            }
        }
        // Do a few of bogus copies
        try {
            wrench::StorageService::copyFile(
                    wrench::FileLocation::LOCATION(this->test->storage_service_1000, nullptr),
                    wrench::FileLocation::LOCATION(this->test->storage_service_100, nullptr));
            throw std::runtime_error("Should not be to able to copy a nullptr file!");
        } catch (std::invalid_argument &) {
        }

        try {
            wrench::StorageService::copyFile(
                    wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_500),
                    nullptr);
            throw std::runtime_error("Should not be able to copy a file to a nullptr location!");
        } catch (std::invalid_argument &) {
        }

        try {
            wrench::StorageService::copyFile(
                    nullptr,
                    wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_500));
            throw std::runtime_error("Should not be able to copy a file from a nullptr location!");
        } catch (std::invalid_argument &) {
        }


        // Copy a file to a storage service that doesn't have enough space
        try {
            wrench::StorageService::copyFile(
                    wrench::FileLocation::LOCATION(this->test->storage_service_1000, this->test->file_500),
                    wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_500));
            throw std::runtime_error(
                    "Should not be able to store a file to a storage service that doesn't have enough capacity");
        } catch (wrench::ExecutionException &e) {
        }

        // Make sure the copy didn't happen
        if (wrench::StorageService::lookupFile(wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_500))) {
            throw std::runtime_error("File copy to a storage service without enough space shouldn't have succeeded");
        }

        if (this->test->storage_service_1000->getFileLastWriteDate(
                    wrench::FileLocation::LOCATION(this->test->storage_service_1000, this->test->file_10)) > 0) {
            throw std::runtime_error("Last file write date of staged file should be 0");
        }

        // Copy a file to a storage service that has enough space
        double before_copy = wrench::Simulation::getCurrentSimulatedDate();
        try {
            wrench::StorageService::copyFile(
                    wrench::FileLocation::LOCATION(this->test->storage_service_1000, this->test->file_10),
                    wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_10));
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Should be able to store a file to a storage service that has enough capacity");
        }
        double after_copy = wrench::Simulation::getCurrentSimulatedDate();

        double last_file_write_date = this->test->storage_service_100->getFileLastWriteDate(
                wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_10));
        if ((last_file_write_date < before_copy) or (last_file_write_date > after_copy)) {
            throw std::runtime_error("Last file write date is incoherent");
        }

        try {
            this->test->storage_service_100->getFileLastWriteDate(nullptr);
            throw std::runtime_error("Should not be able to pass a nullptr location to getFileLastWriteDate()");
        } catch (std::invalid_argument &ignore) {}


        // Send a free space request
        std::map<std::string, double> free_space;
        try {
            free_space = this->test->storage_service_100->getFreeSpace();
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Should be able to get a storage's service free space");
        }
        if ((free_space.size() != 1) or (free_space["/disk100"] != 90.0)) {
            throw std::runtime_error(
                    "Free space on storage service is wrong (" + std::to_string(free_space["/"]) + ") instead of 90.0");
        }

        // Bogus read
        try {
            wrench::StorageService::readFile(wrench::FileLocation::LOCATION(this->test->storage_service_100, nullptr));
            throw std::runtime_error("Should not be able to read nullptr file");
        } catch (std::invalid_argument &e) {
        }

        // Read a file on a storage service
        try {
            wrench::StorageService::readFile(wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_10));
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Should be able to read a file available on a storage service");
        }

        // Read a file on a storage service that doesn't have that file
        try {
            wrench::StorageService::readFile(wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_100));
            throw std::runtime_error("Should not be able to read a file unavailable a storage service");
        } catch (wrench::ExecutionException &e) {
        }

        {// Test using readFiles()

            // Bogus read
            try {
                std::map<std::shared_ptr<wrench::DataFile>, std::shared_ptr<wrench::FileLocation>> locations;
                locations[nullptr] = wrench::FileLocation::LOCATION(this->test->storage_service_100, nullptr);
                wrench::StorageService::readFiles(locations);
                throw std::runtime_error("Should not be able to read nullptr file");
            } catch (std::invalid_argument &e) {
            }

            // Read a file on a storage service
            try {
                std::map<std::shared_ptr<wrench::DataFile>, std::shared_ptr<wrench::FileLocation>> locations;
                locations[this->test->file_10] = wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_10);
                wrench::StorageService::readFiles(locations);
            } catch (wrench::ExecutionException &e) {
                throw std::runtime_error("Should be able to read a file available on a storage service");
            }

            // Read a file on a storage service that doesn't have that file
            try {
                std::map<std::shared_ptr<wrench::DataFile>, std::shared_ptr<wrench::FileLocation>> locations;
                locations[this->test->file_100] = wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_100);
                wrench::StorageService::readFiles(locations);
                throw std::runtime_error("Should not be able to read a file unavailable a storage service");
            } catch (wrench::ExecutionException &e) {
            }
        }

        {// Test using writeFiles()

            // Bogus write
            try {
                std::map<std::shared_ptr<wrench::DataFile>, std::shared_ptr<wrench::FileLocation>> locations;
                locations[nullptr] = wrench::FileLocation::LOCATION(this->test->storage_service_100, nullptr);
                wrench::StorageService::writeFiles(locations);
                throw std::runtime_error("Should not be able to write nullptr file");
            } catch (std::invalid_argument &e) {
            }
        }


        // Delete a file on a storage service that doesn't have it
        try {
            wrench::StorageService::deleteFile(wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_100));
            throw std::runtime_error("Should not be able to delete a file unavailable a storage service");
        } catch (wrench::ExecutionException &e) {
            auto cause = std::dynamic_pointer_cast<wrench::FileNotFound>(e.getCause());
            if (not cause) {
                throw std::runtime_error("Got an expected exception, but unexpected failure cause: " +
                                         e.getCause()->toString() + " (expected: FileNotFound)");
            }
            if (cause->getLocation()->getStorageService() != this->test->storage_service_100) {
                throw std::runtime_error(
                        "Got the expected 'file not found' exception, but the failure cause does not point to the correct storage service");
            }
            if (cause->getFile() != this->test->file_100) {
                throw std::runtime_error(
                        "Got the expected 'file not found' exception, but the failure cause does not point to the correct file");
            }
        }

        // Delete a file in a bogus path
        try {
            wrench::StorageService::deleteFile(wrench::FileLocation::LOCATION(this->test->storage_service_100, "/disk100/bogus", this->test->file_100));
            throw std::runtime_error("Should not be able to delete a file unavailable a storage service");
        } catch (wrench::ExecutionException &e) {
            auto cause = std::dynamic_pointer_cast<wrench::FileNotFound>(e.getCause());
            if (not cause) {
                throw std::runtime_error("Got an expected 'file not found' exception, but unexpected failure cause: " +
                                         e.getCause()->toString() + " (expected: FileNotFound)");
            }
            if (cause->getLocation()->getStorageService() != this->test->storage_service_100) {
                throw std::runtime_error(
                        "Got the expected 'file not found' exception, but the failure cause does not point to the correct storage service");
            }
            if (cause->getFile() != this->test->file_100) {
                throw std::runtime_error(
                        "Got the expected 'file not found' exception, but the failure cause does not point to the correct file");
            }
        }

        // Delete a file on a storage service that has it
        try {
            wrench::StorageService::deleteFile(
                    wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_10));
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Should  be able to delete a file available a storage service");
        }

        // Check that the storage capacity is back to what it should be
        try {
            free_space = this->test->storage_service_100->getFreeSpace();
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Should be able to get a storage's service free space");
        }

        if ((free_space.size() != 1) or (free_space["/disk100"] != 100.0)) {
            throw std::runtime_error(
                    "Free space on storage service is wrong (" + std::to_string(free_space["/disk100"]) + ") instead of 100.0");
        }

        // Do a bogus asynchronous file copy (file = nullptr);
        try {
            data_movement_manager->initiateAsynchronousFileCopy(
                    wrench::FileLocation::LOCATION(this->test->storage_service_1000, nullptr),
                    wrench::FileLocation::LOCATION(this->test->storage_service_100, nullptr));
            throw std::runtime_error("Shouldn't be able to do an initiateAsynchronousFileCopy with a nullptr file");
        } catch (std::invalid_argument &e) {
        }


        // Do a bogus asynchronous file copy (src = nullptr);
        try {
            data_movement_manager->initiateAsynchronousFileCopy(
                    nullptr,
                    wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_1));
            throw std::runtime_error("Shouldn't be able to do an initiateAsynchronousFileCopy with a nullptr src");
        } catch (std::invalid_argument &e) {
        }

        // Do a bogus asynchronous file copy (dst = nullptr);
        try {
            data_movement_manager->initiateAsynchronousFileCopy(
                    wrench::FileLocation::LOCATION(this->test->storage_service_1000, this->test->file_1),
                    nullptr);
            throw std::runtime_error("Shouldn't be able to do an initiateAsynchronousFileCopy with a nullptr dst");
        } catch (std::invalid_argument &e) {
        }

        // Do a valid asynchronous file copy
        try {
            data_movement_manager->initiateAsynchronousFileCopy(
                    wrench::FileLocation::LOCATION(this->test->storage_service_1000, this->test->file_1),
                    wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_1));
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Error while submitting a file copy operations");
        }


        // Wait for a workflow execution event
        std::shared_ptr<wrench::ExecutionEvent> event;
        try {
            event = this->waitForNextEvent();
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Error while getting an execution event: " + e.getCause()->toString());
        }

        if (not std::dynamic_pointer_cast<wrench::FileCopyCompletedEvent>(event)) {
            throw std::runtime_error("Unexpected workflow execution event: " + event->toString());
        }

        // Check that the copy has happened
        if (!wrench::StorageService::lookupFile(wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_1))) {
            throw std::runtime_error("Asynchronous file copy operation didn't copy the file");
        }

        // Check that the free space has been updated at the destination
        try {
            free_space = this->test->storage_service_100->getFreeSpace();
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Should be able to get a storage's service free space");
        }
        if ((free_space.size() != 1) or (free_space["/disk100"] != 99.0)) {
            throw std::runtime_error(
                    "Free space on storage service is wrong (" + std::to_string(free_space["/'"]) + ") instead of 99.0");
        }


        // Do an INVALID asynchronous file copy (file too big)
        try {
            data_movement_manager->initiateAsynchronousFileCopy(
                    wrench::FileLocation::LOCATION(this->test->storage_service_1000, this->test->file_500),
                    wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_500));
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Error while submitting a file copy operations");
        }

        // Wait for a workflow execution event
        try {
            event = this->waitForNextEvent();
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Error while getting an execution event: " + e.getCause()->toString());
        }

        if (auto real_event = std::dynamic_pointer_cast<wrench::FileCopyFailedEvent>(event)) {
            auto cause = std::dynamic_pointer_cast<wrench::StorageServiceNotEnoughSpace>(real_event->failure_cause);
            if (not cause) {
                throw std::runtime_error("Got expected event but unexpected failure cause: " +
                                         real_event->failure_cause->toString() + " (expected: FileCopyFailedEvent)");
            }
        } else {
            throw std::runtime_error("Unexpected workflow execution event: " + event->toString());
        }

        // Do an INVALID asynchronous file copy (file not there)
        try {
            data_movement_manager->initiateAsynchronousFileCopy(
                    wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_500),
                    wrench::FileLocation::LOCATION(this->test->storage_service_510, this->test->file_500));
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Error while submitting a file copy operations");
        }

        // Wait for a workflow execution event
        try {
            event = this->waitForNextEvent();
        } catch (wrench::ExecutionException &e) {
            throw std::runtime_error("Error while getting an execution event: " + e.getCause()->toString());
        }

        auto real_event = std::dynamic_pointer_cast<wrench::FileCopyFailedEvent>(event);
        if (real_event) {
            auto cause = std::dynamic_pointer_cast<wrench::FileNotFound>(real_event->failure_cause);
            if (not cause) {
                throw std::runtime_error("Got expected event but unexpected failure cause: " +
                                         real_event->failure_cause->toString() + " (expected: FileNotFound)");
            }
        } else {
            throw std::runtime_error("Unexpected workflow execution event: " + event->toString());
        }

        // Do a really bogus file removal
        try {
            wrench::StorageService::deleteFile(
                    wrench::FileLocation::LOCATION(this->test->storage_service_100, nullptr));
            throw std::runtime_error("Should not be able to delete a nullptr file from a location");
        } catch (std::invalid_argument &e) {
        }

        // Shutdown the service
        this->test->storage_service_100->stop();

        // Try to do stuff with a shutdown service
        try {
            wrench::StorageService::lookupFile(
                    wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_1));
            throw std::runtime_error("Should not be able to lookup a file from a DOWN service");
        } catch (wrench::ExecutionException &e) {
            // Check Exception
            auto cause = std::dynamic_pointer_cast<wrench::ServiceIsDown>(e.getCause());
            if (not cause) {
                throw std::runtime_error("Got an exception, as expected, but of the unexpected failure cause: " +
                                         e.getCause()->toString() + " (expected: ServiceIsDown)");
            }
            // Check Exception details
            if (cause->getService() != this->test->storage_service_100) {
                throw std::runtime_error(
                        "Got the expected 'service is down' exception, but the failure cause does not point to the correct storage service");
            }
        }


        try {
            wrench::StorageService::lookupFile(
                    wrench::FileLocation::LOCATION(this->test->storage_service_100, "/disk100", this->test->file_1));
            throw std::runtime_error("Should not be able to lookup a file from a DOWN service");
        } catch (wrench::ExecutionException &e) {
            // Check Exception
            auto cause = std::dynamic_pointer_cast<wrench::ServiceIsDown>(e.getCause());
            if (not cause) {
                throw std::runtime_error("Got an exception, as expected, but an unexpected failure cause: " +
                                         e.getCause()->toString() + " (was expecting ServiceIsDown)");
            }
            // Check Exception details
            if (cause->getService() != this->test->storage_service_100) {
                throw std::runtime_error(
                        "Got the expected 'service is down' exception, but the failure cause does not point to the correct storage service");
            }
        }


        try {
            wrench::StorageService::readFile(
                    wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_1));
            throw std::runtime_error("Should not be able to read a file from a down service");
        } catch (wrench::ExecutionException &e) {
            // Check Exception
            auto cause = std::dynamic_pointer_cast<wrench::ServiceIsDown>(e.getCause());
            if (not cause) {
                throw std::runtime_error("Got an exception, as expected, but of the unexpected failure cause: " +
                                         e.getCause()->toString() + " (was expecting ServiceIsDown)");
            }
            // Check Exception details
            if (cause->getService() != this->test->storage_service_100) {
                throw std::runtime_error(
                        "Got the expected 'service is down' exception, but the failure cause does not point to the correct storage service");
            }
        }

        try {
            wrench::StorageService::writeFile(
                    wrench::FileLocation::LOCATION(this->test->storage_service_100, this->test->file_1));
            throw std::runtime_error("Should not be able to write a file from a DOWN service");
        } catch (wrench::ExecutionException &e) {
            // Check Exception
            auto cause = std::dynamic_pointer_cast<wrench::ServiceIsDown>(e.getCause());
            if (not cause) {
                throw std::runtime_error("Got an exception, as expected, but of the unexpected failure cause: " +
                                         e.getCause()->toString() + " (was expecting ServiceIsDown)");
            }
            // Check Exception details
            if (cause->getService() != this->test->storage_service_100) {
                throw std::runtime_error(
                        "Got the expected 'service is down' exception, but the failure cause does not point to the correct storage service");
            }
        }

        try {
            this->test->storage_service_100->getFreeSpace();
            throw std::runtime_error("Should not be able to get free space info from a DOWN service");
        } catch (wrench::ExecutionException &e) {
            // Check Exception
            auto cause = std::dynamic_pointer_cast<wrench::ServiceIsDown>(e.getCause());
            if (not cause) {
                throw std::runtime_error("Got an exception, as expected, but of the unexpected failure cause: " +
                                         e.getCause()->toString() + " (was expecting ServiceIsDown)");
            }
            // Check Exception details
            wrench::ServiceIsDown *real_cause = (wrench::ServiceIsDown *) e.getCause().get();
            if (real_cause->getService() != this->test->storage_service_100) {
                throw std::runtime_error(
                        "Got the expected 'service is down' exception, but the failure cause does not point to the correct storage service");
            }
        }
        */

        return 0;
    }
};

TEST_F(CompoundStorageServiceFunctionalTest, BasicFunctionality) {
    DO_TEST_WITH_FORK(do_BasicFunctionality_test);
}

void CompoundStorageServiceFunctionalTest::do_BasicFunctionality_test() {

    // Create and initialize a simulation
    auto simulation = wrench::Simulation::createSimulation();

    int argc = 1;
    char **argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    //    argv[1] = strdup("--wrench-full-log");

    ASSERT_NO_THROW(simulation->init(&argc, argv));

    // Setting up the platform
    ASSERT_NO_THROW(simulation->instantiatePlatform(platform_file_path));

    // Get a hostname
    auto compute = "ComputeHost";
    auto simple_storage0 = "SimpleStorageHost0";
    auto simple_storage1 = "SimpleStorageHost1";
    auto compound_storage = "CompoundStorageHost";

    // Create a Compute Service
    ASSERT_NO_THROW(compute_service = simulation->add(
                            new wrench::BareMetalComputeService(compute,
                                                                {std::make_pair(compute, std::make_tuple(wrench::ComputeService::ALL_CORES,
                                                                                                          wrench::ComputeService::ALL_RAM))},
                                                                {})));

    // Create some simple storage services
    ASSERT_NO_THROW(simple_storage_service_100 = simulation->add(
                        wrench::SimpleStorageService::createSimpleStorageService(simple_storage0, {"/disk100"},
                            {{wrench::SimpleStorageServiceProperty::BUFFER_SIZE, "10000"}}, {})));

    ASSERT_NO_THROW(simple_storage_service_510 = simulation->add(
                        wrench::SimpleStorageService::createSimpleStorageService(simple_storage1, {"/disk100", "/disk510"},
                            {{wrench::SimpleStorageServiceProperty::BUFFER_SIZE, "10000"}}, {})));

    // Create a bad Compound Storage Service (no storage services)
    ASSERT_THROW(compound_storage_service = simulation->add(
                    new wrench::CompoundStorageService(compound_storage, {})),
                 std::invalid_argument);

    // Create a bad Compound Storage Service (nullptr storage service)
    ASSERT_THROW(compound_storage_service = simulation->add(
                    new wrench::CompoundStorageService(compound_storage, {simple_storage_service_1000})),
                std::invalid_argument);

    // Create a bad Compound Storage Service with a bogus property
    ASSERT_THROW(compound_storage_service = simulation->add(
                    new wrench::CompoundStorageService(compound_storage, {simple_storage_service_100}, {{wrench::CompoundStorageServiceProperty::STORAGE_SELECTION_METHOD, "round_robin"}}, {})),
                 std::invalid_argument);

    // Create a valid Compound Storage Service with a bogus property
    ASSERT_NO_THROW(compound_storage_service = simulation->add(
                        new wrench::CompoundStorageService(compound_storage, {simple_storage_service_100, simple_storage_service_510})));

    // Create a Controler
    std::shared_ptr<wrench::ExecutionController> wms = nullptr;
    ASSERT_NO_THROW(wms = simulation->add(
                            new CompoundStorageServiceBasicFunctionalityTestCtrl(this, compound_storage)));

    // A bogus staging (can't use CompoundStorageService for staging)
    ASSERT_THROW(simulation->stageFile(file_10, compound_storage_service), std::invalid_argument);

    // Running a "run a single task1" simulation
    ASSERT_NO_THROW(simulation->launch());


    for (int i = 0; i < argc; i++)
        free(argv[i]);
    free(argv);
}