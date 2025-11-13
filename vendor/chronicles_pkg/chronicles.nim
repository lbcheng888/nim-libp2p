import "../../nimbledeps/pkgs2/chronicles-0.11.0-e5be8a1a1d79df93be25d7f636d867c70e4ab352/chronicles" as base
import "../../nimbledeps/pkgs2/chronicles-0.11.0-e5be8a1a1d79df93be25d7f636d867c70e4ab352/chronicles/log_output" as base_log_output

export base

when not declared(defaultChroniclesStream):
  base_log_output.logStream defaultChroniclesStream[
    textlines[stdout]
  ]
