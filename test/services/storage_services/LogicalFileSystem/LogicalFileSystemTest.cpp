#include <gtest/gtest.h>
#include <wrench-dev.h>

#include "../../../include/TestWithFork.h"
#include "../../../include/UniqueTmpPathPrefix.h"

WRENCH_LOG_CATEGORY(logical_file_system_test, "Log category for LogicalFileSystemTest");


class LogicalFileSystemTest : public ::testing::Test {

public:
    void do_BasicTests();
    void do_DevNullTests();
    void do_LRUTests();

protected:
    LogicalFileSystemTest() {

        // Create a 2-host platform file
        // [WMSHost]-----[StorageHost]
        std::string xml = "<?xml version='1.0'?>"
                          "<!DOCTYPE platform SYSTEM \"https://simgrid.org/simgrid.dtd\">"
                          "<platform version=\"4.1\"> "
                          "   <zone id=\"AS0\" routing=\"Full\"> "
                          "       <host id=\"Host\" speed=\"1f\"> "
                          "          <disk id=\"large_disk1\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"100MB\"/>"
                          "             <prop id=\"mount\" value=\"/\"/>"
                          "          </disk>"
                          "          <disk id=\"100bytedisk\" read_bw=\"100MBps\" write_bw=\"100MBps\">"
                          "             <prop id=\"size\" value=\"100\"/>"
                          "             <prop id=\"mount\" value=\"/tmp\"/>"
                          "          </disk>"
                          "       </host>"
                          "   </zone> "
                          "</platform>";
        FILE *platform_file = fopen(platform_file_path.c_str(), "w");
        fprintf(platform_file, "%s", xml.c_str());
        fclose(platform_file);
    }

    std::string platform_file_path = UNIQUE_TMP_PATH_PREFIX + "platform.xml";
};


TEST_F(LogicalFileSystemTest, BasicTests) {
    DO_TEST_WITH_FORK(do_BasicTests);
}

void LogicalFileSystemTest::do_BasicTests() {
    // Create and initialize the simulation
    auto simulation = wrench::Simulation::createSimulation();

    int argc = 1;
    char **argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    //    argv[1] = strdup("--wrench-full-log");

    ASSERT_NO_THROW(simulation->init(&argc, argv));
    auto workflow = wrench::Workflow::createWorkflow();

    // set up the platform
    ASSERT_NO_THROW(simulation->instantiatePlatform(platform_file_path));

    ASSERT_THROW(new wrench::LogicalFileSystem("Host", nullptr, "/"), std::invalid_argument);

    // Create two Storage Services
    std::shared_ptr<wrench::SimpleStorageService> storage_service1, storage_service2;
    ASSERT_NO_THROW(storage_service1 = simulation->add(
                            wrench::SimpleStorageService::createSimpleStorageService("Host", {"/"})));
    ASSERT_NO_THROW(storage_service2 = simulation->add(
                            wrench::SimpleStorageService::createSimpleStorageService("Host", {"/"})));

    // Create a Logical File System
    auto fs1 = new wrench::LogicalFileSystem("Host", storage_service1.get(), "/");
    fs1->init();

    // Attempt to create a redundant Logical File System
    auto fs1_bogus = new wrench::LogicalFileSystem("Host", storage_service2.get(), "/");
    try {
        fs1_bogus->init();
        throw std::runtime_error("Initializing a redundant file system should have thrown");
    } catch (std::invalid_argument &e) {
        //  ignored
    }

    ASSERT_THROW(new wrench::LogicalFileSystem("Host", storage_service1.get(), "/bogus"), std::invalid_argument);

    fs1->createDirectory(("/foo"));
    fs1->removeAllFilesInDirectory("/foo");
    fs1->listFilesInDirectory("/foo");
    fs1->removeEmptyDirectory("/foo");

    auto file = workflow->addFile("file", 10000000000);
    auto file1 = workflow->addFile("file1", 10000);
    ASSERT_THROW(fs1->reserveSpace(file, "/files/"), std::invalid_argument);

    workflow->clear();

    for (int i = 0; i < argc; i++)
        free(argv[i]);
    free(argv);
}


TEST_F(LogicalFileSystemTest, DevNullTests) {
    DO_TEST_WITH_FORK(do_DevNullTests);
}

void LogicalFileSystemTest::do_DevNullTests() {
    // Create and initialize the simulation
    auto simulation = wrench::Simulation::createSimulation();

    int argc = 1;
    char **argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    //    argv[1] = strdup("--wrench-full-log");

    ASSERT_NO_THROW(simulation->init(&argc, argv));
    auto workflow = wrench::Workflow::createWorkflow();

    // set up the platform
    ASSERT_NO_THROW(simulation->instantiatePlatform(platform_file_path));

    // Create a  Storage Services
    std::shared_ptr<wrench::SimpleStorageService> storage_service;
    ASSERT_NO_THROW(storage_service = simulation->add(
                            wrench::SimpleStorageService::createSimpleStorageService("Host", {"/"})));

    // Create a Logical File System
    auto fs1 = new wrench::LogicalFileSystem("Host", storage_service.get(), "/dev/null");
    fs1->init();

    auto file = wrench::Simulation::addFile("file", 1.0);

    fs1->createDirectory(("/foo"));
    ASSERT_FALSE(fs1->doesDirectoryExist(("/foo")));
    ASSERT_TRUE(fs1->isDirectoryEmpty(("/foo")));
    ASSERT_FALSE(fs1->isFileInDirectory(file, "/foo"));
    fs1->removeEmptyDirectory("/foo");
    fs1->storeFileInDirectory(file, "/foo");
    fs1->removeFileFromDirectory(file, "/foo");
    fs1->removeAllFilesInDirectory("/foo");
    ASSERT_TRUE(fs1->listFilesInDirectory("/foo").empty());
    fs1->reserveSpace(file, "/foo");
    fs1->unreserveSpace(file, "/foo");
    fs1->getFileLastWriteDate(file, "/foo");

    workflow->clear();

    for (int i = 0; i < argc; i++)
        free(argv[i]);
    free(argv);
}


TEST_F(LogicalFileSystemTest, LRUTests) {
    DO_TEST_WITH_FORK(do_LRUTests);
}

void LogicalFileSystemTest::do_LRUTests() {
    // Create and initialize the simulation
    auto simulation = wrench::Simulation::createSimulation();

    int argc = 1;
    char **argv = (char **) calloc(argc, sizeof(char *));
    argv[0] = strdup("unit_test");
    //    argv[1] = strdup("--wrench-full-log");

    ASSERT_NO_THROW(simulation->init(&argc, argv));
    auto workflow = wrench::Workflow::createWorkflow();

    // set up the platform
    ASSERT_NO_THROW(simulation->instantiatePlatform(platform_file_path));

    // Create a  Storage Services
    std::shared_ptr<wrench::SimpleStorageService> storage_service;
    ASSERT_NO_THROW(storage_service = simulation->add(
                            wrench::SimpleStorageService::createSimpleStorageService("Host", {"/tmp"})));

    // Create a Logical File System with LRU eviction
    auto fs1 = new wrench::LogicalFileSystem("Host", storage_service.get(), "/tmp", "LRU");
    fs1->init();

    auto file_60 = wrench::Simulation::addFile("file_60", 60);
    auto file_50 = wrench::Simulation::addFile("file_50", 50);
    auto file_30 = wrench::Simulation::addFile("file_30", 30);
    auto file_20 = wrench::Simulation::addFile("file_20", 20);
    auto file_10 = wrench::Simulation::addFile("file_10", 10);

    fs1->createDirectory(("/foo"));
    ASSERT_TRUE(fs1->reserveSpace(file_60, "/foo"));
    ASSERT_FALSE(fs1->reserveSpace(file_50, "/foo"));
    fs1->storeFileInDirectory(file_60, "/foo");
    fs1->storeFileInDirectory(file_10, "/foo");

    ASSERT_TRUE(fs1->reserveSpace(file_50, "/foo"));
    // Check that file_60 has been evicted
    ASSERT_FALSE(fs1->isFileInDirectory(file_60, "/foo"));
    // Check that file_10 is still there evicted
    ASSERT_TRUE(fs1->isFileInDirectory(file_10, "/foo"));
    fs1->storeFileInDirectory(file_50, "/foo");


    workflow->clear();

    for (int i = 0; i < argc; i++)
        free(argv[i]);
    free(argv);
}
