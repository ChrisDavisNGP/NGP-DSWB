# NGP-DSWB
Julia source code for Akamai Soasta DSWB applications


--------------------------------------------------------------------------
Nov 15, 2017

New rules
- Notebooks
  - are the shell with as little permanent code as possible
  - stored in Github in rpt folder under name of report folder
  - e.g. NGP-DSWB/Rpt/Find-A-Page-View-Spike/Find-A-Page-View-Spike.ipynb
- Body files
  - are all the code unique to a report
  - change version number when you start a new series of changes
  - if more than one body needs the code put it in the library
  - stored in Rpt directory under folder named for report
  - e.g. NGP-DSWB/Rpt/Find-A-Page-View-Spike/Find-A-Page-View-Spike-Body.jl
- Library files
  - common functions
--------------------------------------------------------------------------
Old and New Style For ipynb includes

#include("/data/notebook_home/Production/Lib/Include-Package-v1.0.jl")

run(\`rm -rf NGP-DSWB\`)
run(\`git clone https://github.com/ChrisDavisNGP/NGP-DSWB.git\`)
include("NGP-DSWB/Lib/Include-Package-v2.0.jl")

#run(\`pwd\`)
#run(\`ls NGP-DSWB/Lib\`)
#run(\`ls /data/notebook_home/Production/Studies/Sessions/NGP-DSWB/Lib\`)

To get "Bodies" I switched to "NB-Lib" or notebook library

include("NGP-DSWB/NB-Lib/Individual-Streamline-v1.2.jl")

Is the new path.


---------------------------------------------------------------------------
