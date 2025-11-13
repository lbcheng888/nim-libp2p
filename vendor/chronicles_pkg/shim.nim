import chronicles
import chronicles/log_output

logStream defaultChroniclesStream[
  textlines[stdout]
]

export chronicles
