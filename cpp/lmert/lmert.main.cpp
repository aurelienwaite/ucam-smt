#include <main.custom_assert.hpp>
#include <main.logger.hpp>
#include <main.lmert.hpp>

int main ( int argc,  const char* argv[] ) {
	using namespace ucam::util;
	using namespace ucam::fsttools;
	using namespace ucam::lmert;
  initLogger ( argc, argv );
  FORCELINFO ( argv[0] << " starts!" );
  RegistryPO rg ( argc, argv );
  FORCELINFO ( rg.dump ( "CONFIG parameters:\n=====================",
                         "=====================" ) );
  BleuScorer bleuScorer ( rg , HifstConstants::kLmertRefs );
  TuneSet< TupleArc32 > tuneSet ( rg );
  PARAMS32 lambda = ucam::util::ParseParamString<float> 
		(rg.getString( HifstConstants::kLmertInitialParams ));
  RandomLineSearch< TupleArc32 > rls ( rg, tuneSet, bleuScorer, lambda );
  FORCELINFO ( argv[0] << " finished!" );
}