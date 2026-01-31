// DictionaryArrayTests.swift
// Arrow
//
// Created by Will Temperley on 30/01/2026. All rights reserved.
// Copyright 2026 Will Temperley.
//
// Copying or reproduction of this file via any medium requires prior express
// written permission from the copyright holder.
// -----------------------------------------------------------------------------
///
/// Implementation notes, links and internal documentation go here.
///
// -----------------------------------------------------------------------------

import Foundation
import Testing

@testable import Arrow

struct DictionaryArrayTests {

  @Test func basicArrayTests() throws {

    let keyArrayBuilder: ArrayBuilderNumeric<Int32> = .init()
    keyArrayBuilder.append(0)
    keyArrayBuilder.append(1)
    keyArrayBuilder.append(2)
    keyArrayBuilder.append(3)
    keyArrayBuilder.append(4)
    let keyArray = keyArrayBuilder.finish()

    let valueArrayBuilder: ArrayBuilderVariableLength<String, Int32> = .init()
    valueArrayBuilder.append("A")
    valueArrayBuilder.append("B")
    valueArrayBuilder.append("C")
    valueArrayBuilder.append("D")
    valueArrayBuilder.append("E")
    let valuesArray = valueArrayBuilder.finish()

    let dictionaryArray = ArrowDictionaryArray<Int32>(
      length: 5,
      keys: keyArray,
      values: valuesArray
    )

    #expect(dictionaryArray[2] as? String == "C")
    #expect(dictionaryArray[3] as? String == "D")

    let valueArrayBuilder2: ArrayBuilderVariableLength<String, Int32> = .init()
    valueArrayBuilder2.append("F")
    valueArrayBuilder2.append("G")
    valueArrayBuilder2.append("H")
    valueArrayBuilder2.append("I")
    valueArrayBuilder2.appendNull()
    let valuesArray2 = valueArrayBuilder2.finish()
    dictionaryArray.values.currentArray = valuesArray2

    #expect(dictionaryArray[2] as? String == "H")
    #expect(dictionaryArray[3] as? String == "I")
    #expect(dictionaryArray[4] == nil)
  }

}
