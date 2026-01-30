// Copyright 2025 The Apache Software Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import Arrow
import BinaryParsing

struct MessageHeader {
  let metadataLength: UInt32
  var isEndOfStream: Bool { metadataLength == 0 }

  @inlinable
  init(parsing input: inout ParserSpan) throws {
    let continuation = try UInt32(parsingLittleEndian: &input)
    guard continuation == continuationMarker else {
      throw ArrowError(.invalid("Missing continuation marker."))
    }
    self.metadataLength = try UInt32(parsingLittleEndian: &input)
  }
}
