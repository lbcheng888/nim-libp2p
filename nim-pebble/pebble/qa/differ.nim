# QA 差分工具，复用批处理互操作模块生成状态差异。

import pebble/batch/interop as batch_interop
proc diffStates*(lhs, rhs: batch_interop.BatchState): seq[string] =
  ## 封装 batch/interop.diffStates，便于 QA 模块调用。
  batch_interop.diffStates(lhs, rhs)
