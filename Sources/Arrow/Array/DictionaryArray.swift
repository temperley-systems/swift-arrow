// Copyright 2026 The Columnar Swift Contributors
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

/// A shared container for the dictionary values.
public class DictionaryValues: @unchecked Sendable {
  var currentArray: AnyArrowArrayProtocol

  init(_ array: AnyArrowArrayProtocol) {
    self.currentArray = array
  }
}

///// An Arrow dictionary array.
public struct ArrowDictionaryArray<
  IndexType: FixedWidthInteger & BitwiseCopyable
>: ArrowArrayProtocol {
  public let offset: Int
  public let length: Int
  public var bufferSizes: [Int] { keys.bufferSizes }
  public var buffers: [ArrowBufferProtocol] { keys.buffers }
  public var nullCount: Int { keys.nullCount }

  public let keys: ArrowArrayNumeric<IndexType>
  public let values: DictionaryValues

  public init(
    offset: Int = 0,
    length: Int,
    keys: ArrowArrayNumeric<IndexType>,
    values: AnyArrowArrayProtocol
  ) {
    self.offset = offset
    self.length = length
    self.keys = keys
    self.values = DictionaryValues(values)
  }

  public subscript(index: Int) -> Any? {
    precondition(index >= 0 && index < length, "Invalid index.")
    let offsetIndex = self.offset + index
    guard let key = keys[offsetIndex] else {
      return nil
    }
    precondition(
      Int(key) < values.currentArray.length, "Key out of bounds for dictionary")
    return values.currentArray.any(at: Int(key))
  }

  public func slice(offset: Int, length: Int) -> Self {
    .init(
      offset: 0,
      length: length,
      keys: keys.slice(offset: self.offset + offset, length: length),
      values: values.currentArray
    )
  }
}
