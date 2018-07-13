#include <gtest/gtest.h>
#include <wrench-dev.h>

#include "../../include/TestWithFork.h"


class SimpleStorageServiceZeroSizeFileTest : public ::testing::Test {

public:
    wrench::WorkflowFile *file;

    wrench::StorageService *storage_service = nullptr;

    void do_ReadZeroSizeFileTest();

protected:
    SimpleStorageServiceZeroSizeFileTest() {
        // simple workflow
        workflow = new wrench::Workflow();

        // create the files
        file = workflow->addFile("file_1", 0);


        // Create a 2-host platform file
        // [WMSHost]-----[StorageHost]
        std::string xml = "<?xml version='1.0'?>"
                          "<!DOCTYPE platform SYSTEM \"http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd\">"
                          "<platform version=\"4.1\"> "
                          "   <zone id=\"AS0\" routing=\"Full\"> "
                          "       <host id=\"StorageHost\" speed=\"1f\"/> "
                          "       <host id=\"WMSHost\" speed=\"1f\"/> "
                          "       <link id=\"link\" bandwidth=\"10MBps\" latency=\"100us\"/>"
                          "       <route src=\"WMSHost\" dst=\"StorageHost\">"
                          "         <link_ctn id=\"link\"/>"
                          "       </route>"
                          "   </zone> "
                          "</platform>";
        FILE *platform_file = fopen(platform_file_path.c_str(), "w");
        fprintf(platform_file, "%s", xml.c_str());
        fclose(platform_file);
    }

    std::string platform_file_path = "/tmp/platform.xml";
    wrench::Workflow *workflow;
};

class SimpleStorageServiceZeroSizeFileTestWMS : public wrench::WMS {
public:
    SimpleStorageServiceZeroSizeFileTestWMS(SimpleStorageServiceZeroSizeFileTest *test,
                                              const std::set<wrench::StorageService *> &storage_services,
                                              wrench::FileRegistryService * file_registry_service,
                                              std::string hostname) :
            wrench::WMS(nullptr, nullptr, {}, storage_services, {}, file_registry_service,
                        hostname, "test") {
        this->test = test;
    }

private:
    SimpleStorageServiceZeroSizeFileTest *test;

    int main() {

        // get the file registry service
        wrench::FileRegistryService *file_registry_service = this->getAvailableFileRegistryService();

        // get the single storage service
        wrench::StorageService *storage_service = *(this->getAvailableStorageServices().begin());

        // read the file
        storage_service->readFile(this->test->file);


        return 0;
    }
};

TEST_F(SimpleStorageServiceZeroSizeFileTest, ReadZeroSizeFile) {
    DO_TEST_WITH_FORK(do_ReadZeroSizeFileTest);
}

void SimpleStorageServiceZeroSizeFileTest::do_ReadZeroSizeFileTest() {
    // Create and initialize the simulation
    wrench::Simulation *simulation = new wrench::Simulation();

    int argc = 1;
    char **argv = (char **) calloc(1, sizeof(char *));
    argv[0] = strdup("delete_register_test");

    ASSERT_NO_THROW(simulation->init(&argc, argv));

    // set up the platform
    ASSERT_NO_THROW(simulation->instantiatePlatform(platform_file_path));

    // Create One Storage Service
    ASSERT_NO_THROW(storage_service = simulation->add(
            new wrench::SimpleStorageService("StorageHost", 10)));

    // Create a file registry
    wrench::FileRegistryService *file_registry_service = nullptr;
    ASSERT_NO_THROW(file_registry_service = simulation->add(new wrench::FileRegistryService("WMSHost")));

    // Create a WMS
    wrench::WMS *wms = nullptr;
    ASSERT_NO_THROW(wms = simulation->add(new SimpleStorageServiceZeroSizeFileTestWMS(
            this, {storage_service}, file_registry_service, "WMSHost")));

    wms->addWorkflow(this->workflow);

    // Stage the file on the StorageHost
    ASSERT_NO_THROW(simulation->stageFiles({{file->getID(), file}}, storage_service));

    ASSERT_NO_THROW(simulation->launch());

    delete simulation;
    free(argv[0]);
    free(argv);
}
