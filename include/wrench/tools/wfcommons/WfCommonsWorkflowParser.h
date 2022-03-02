/**
 * Copyright (c) 2017-2019. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef WRENCH_WFCOMMONSWORKFLOWPARSER_H
#define WRENCH_WFCOMMONSWORKFLOWPARSER_H

#include <string>
#include <memory>

namespace wrench {

    class Workflow;

    /**
     * @brief A class that implement methods to read workflow files 
     *        provided by the WfCommons project
     */
    class WfCommonsWorkflowParser {

    public:

        /**
         * @brief Create an abstract workflow based on a JSON file
         *
         * @param filename: the path to the JSON file
         * @param reference_flop_rate: a reference compute speed (in flops/sec), assuming a task's computation is purely flops.
         *                             This is needed because JSON files specify task execution times in seconds,
         *                             but the WRENCH simulation needs some notion of "amount of computation" to
         *                             apply reasonable scaling. (Because the XML platform description specifies host
         *                             compute speeds in flops/sec). The times in the JSON file are thus assumed to be
         *                             obtained on an machine with flop rate reference_flop_rate. NOTE: This is only used
         *                             if the JSON file does not provide information regarding the machine on which a task
         *                             was executed. In this case, the machine speed information is used.
         * @param redundant_dependencies: Workflows provided by WfCommons
         *                             sometimes include control/data dependencies between tasks that are already induced by
         *                             other control/data dependencies (i.e., they correspond to transitive
         *                             closures or existing edges in the workflow graphs). Passing redundant_dependencies=true
         *                             force these "redundant" dependencies to be added as edges in the workflow. Passing
         *                             redundant_dependencies=false will ignore these "redundant" dependencies. Most users
         *                             would likely pass "false".
         * @param min_cores_per_task: If the JSON file does not specify a number of cores for a task, the minimum number of
         *                            cores on which the task can run is set to this value. (default is 1)
         * @param max_cores_per_task: If the JSON file does not specify a number of cores for a task, the maximum number of
         *                            cores on which the task can run is set to this value. (default is 1)
         * @param enforce_num_cores: Use the min_cores_per_task and max_cores_per_task values even if the JSON file specifies
         *                           a number of cores for a task. (default is false)
         * @return a workflow
         * @throw std::invalid_argument
         *
         */

        static std::shared_ptr<Workflow> createWorkflowFromJSON(const std::string &filename, const std::string &reference_flop_rate,
                                                bool redundant_dependencies = false,
                                                unsigned long min_cores_per_task = 1,
                                                unsigned long max_cores_per_task = 1,
                                                bool enforce_num_cores = false);

        /**
          * @brief Create an NON-abstract workflow based on a JSON file
          *
          * @param filename: the path to the JSON file
          * @param reference_flop_rate: a reference compute speed (in flops/sec), assuming a task's computation is purely flops.
          *                             This is needed because JSON files specify task execution times in seconds,
          *                             but the WRENCH simulation needs some notion of "amount of computation" to
          *                             apply reasonable scaling. (Because the XML platform description specifies host
          *                             compute speeds in flops/sec). The times in the JSON file are thus assumed to be
          *                             obtained on an machine with flop rate reference_flop_rate.
          * @param redundant_dependencies: Workflows provided by WfCommons
          *                             sometimes include control/data dependencies between tasks that are already induced by
          *                             other control/data dependencies (i.e., they correspond to transitive
          *                             closures or existing edges in the workflow graphs). Passing redundant_dependencies=true
          *                             force these "redundant" dependencies to be added as edges in the workflow. Passing
          *                             redundant_dependencies=false will ignore these "redundant" dependencies. Most users
          *                             woudl likely pass "false".
          * @param min_cores_per_task: If the JSON file does not specify a number of cores for a task, the minimum number of
          *                            cores on which the task can run is set to this value. (default is 1)
          * @param max_cores_per_task: If the JSON file does not specify a number of cores for a task, the maximum number of
          *                            cores on which the task can run is set to this value. (default is 1)
          * @param enforce_num_cores: Use the min_cores_per_task and max_cores_per_task values even if the JSON file specifies
          *                           a number of cores for a task. (default is false)
          *
          * @return a workflow
          * @throw std::invalid_argument
          */
        static std::shared_ptr<Workflow> createExecutableWorkflowFromJSON(const std::string &filename, const std::string &reference_flop_rate,
                                                          bool redundant_dependencies = false,
                                                          unsigned long min_cores_per_task = 1,
                                                          unsigned long max_cores_per_task = 1,
                                                          bool enforce_num_cores = false);

    };

};


#endif //WRENCH_WFCOMMONSWORKFLOWPARSER_H
