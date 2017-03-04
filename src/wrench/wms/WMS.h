/**
 *  @file    WMS.h
 *  @author  Henri Casanova
 *  @date    2/24/2017
 *  @version 1.0
 *
 *  @brief WRENCH::WMS class implementation
 *
 *  @section DESCRIPTION
 *
 *  The WRENCH::WMS is a mostly abstract implementation of a WMS
 *
 */


#ifndef WRENCH_WMS_H
#define WRENCH_WMS_H

#include "workflow/Workflow.h"

namespace WRENCH {

		class WMS {

		public:
				WMS(Workflow *w);

		private:
				Workflow *workflow;

		};

};


#endif //WRENCH_WMS_H
