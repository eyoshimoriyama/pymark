from CohortAnalysis.cohortmanager import *

cm = CohortManager(source='data-general', destination='data-general')
cm.dumpCohortedAdsetRev()
cm.dumpCohortedCountryRev()
