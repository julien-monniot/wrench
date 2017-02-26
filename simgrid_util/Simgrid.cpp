/**
 *  @file    Simgrid.cpp
 *  @author  Henri Casanova
 *  @date    2/25/2017
 *  @version 1.0
 *
 *  @brief WRENCH::Simgrid class implementations
 *
 *  @section DESCRIPTION
 *
 *  The WRENCH::Simgrid is a MSG wrapper
 *
 */


#include "Simgrid.h"

#include <simgrid/msg.h>

namespace WRENCH {

		void Simgrid::initialize(int *argc, char **argv) {
			MSG_init(argc, argv);
		}

		void Simgrid::runSimulation() {
			MSG_main();
		}

};