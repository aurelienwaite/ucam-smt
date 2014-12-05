// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use these files except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2012 - Gonzalo Iglesias, Adrià de Gispert, William Byrne

#define HIFST

/**
 * \file
 * \brief Hifst main entry file.
 * \date 8-8-2012
 * \author Gonzalo Iglesias
 */

///Include all necessary headers here.
#include <main.hifst.hpp>
#include <main.custom_assert.hpp>
#include <main.logger.hpp>
#include <main-run.hifst.hpp>
#include <common-helpers.hpp>

/**
 * @brief Concrete RunTaskT implementation for Hifst tool.
 */
template < template <class, class> class DataT
           , class KenLMModelT
           , class ArcT
           >
struct RunHifst {
  explicit RunHifst(ucam::util::RegistryPO const &rg){
  using ucam::hifst::SingleThreadedHifstTask;
  using ucam::hifst::MultiThreadedHifstTask;
  using ucam::hifst::HifstServerTask;
  using ucam::fsttools::RunTask3;
  (RunTask3<SingleThreadedHifstTask
          , MultiThreadedHifstTask
          , HifstServerTask
          , DataT
          , KenLMModelT
          , ArcT >
   (rg) );
  }
};

void ucam::fsttools::MainClass::run() {
  using namespace HifstConstants;
  std::string const arctype =rg_->get<std::string>(kHifstSemiring);
  using namespace ucam::hifst;
  if (arctype == kHifstSemiringLexStdArc || arctype == kHifstSemiringStdArc) {
    runTaskWithKenLMTemplate<RunHifst, HifstTaskData, fst::LexStdArc>(*rg_);
  } else if (arctype == HifstConstants::kHifstSemiringTupleArc)
    runTaskWithKenLMTemplate<RunHifst, HifstTaskData, TupleArc32>(*rg_);
}

/**
 * \brief Main function.
 * \param       argc: Number of command-line program options.
 * \param       argv: Actual program options.
 * \remarks     Main function. Runs hifst tool.
 * First parses program options with boost, then loads and chains several task classes.
 * Finally, kick off translation for a range of sentences.
 */
int main ( int argc, const char *argv[] ) {
  (ucam::fsttools::MainClass(argc,argv).run());
  return 0;
}
