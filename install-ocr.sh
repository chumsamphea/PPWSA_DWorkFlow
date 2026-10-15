#!/bin/sh
cd "$(dirname "$0")" && python3 -m pip install --upgrade rapidocr_onnxruntime || python3 -m pip install --upgrade rapidocr onnxruntime
