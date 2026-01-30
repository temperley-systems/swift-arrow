#!/bin/bash
container run -v "$(pwd)":/src flatc File.fbs Message.fbs Schema.fbs SparseTensor.fbs Tensor.fbs
