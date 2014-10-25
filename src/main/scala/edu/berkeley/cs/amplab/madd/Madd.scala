/*
 * Copyright (c) 2014. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.madd

import edu.berkeley.cs.amplab.madd.models._
import edu.berkeley.cs.amplab.madd.util.FitsUtils
import java.io._

object Madd {
  def main(args: Array[String]) {

    /**
     * Given an array of Fits datasets, maps them into a 2D matrix and
     * averages out the values.
     *
     * @param fitsList
     */
    def add(fitsList: Array[Fits],
            metadata: Map[Int, FitsMetadata],
            tcol: Int,
            trow: Int): Array[Array[Float]] = {

      // create and initialize array
      val matrix = Array.ofDim[Float](trow, tcol)
      for {
        i <- (0 until trow)
        j <- (0 until tcol)
      } matrix(i)(j) = Float.NaN

      val l = (1 to trow).map(i => (0 until fitsList.length)
        .filter(x => i - metadata(x).start >= 0 && i - metadata(x).end < 0))
      val lmap = (1 to trow).zip(l)

      lmap.map(kv => {
        val (row, flist) = kv

        // initialize an array with NaNs
        var array2d = Array.fill[Float](flist.length, tcol)(Float.NaN)

        flist.zipWithIndex.map(kv => {
          val (x, i) = kv

          // get offset and start
          val offset = metadata(x).offset
          val start = metadata(x).start

          // copy columns into the 2d matrix
          Array.copy(fitsList(x).matrix(row - start), 0, array2d(i), offset, fitsList(x).ncol)
        })

        // averages the values of a single column
        def avgColumn(c: Int): Float = {
          // get the c'th element from each row that isn't a NaN
          val cThNonNaNs = array2d.map(r => r(c))
            .filter(!_.isNaN)

          // get the number of non-NaN values in that column
          val count = cThNonNaNs.length

          // if we have more than one value, average it, else emit a NaN
          if (count > 0) {
            cThNonNaNs.sum / count
          } else {
            Float.NaN
          }
        }

        // average out the columns
        var array = (0 until tcol).map(avgColumn).toArray

        // copy the averaged array back into the matrix
        Array.copy(array, 0, matrix(row - 1), 0, tcol)
      })

      // return the averaged matrix
      matrix
    }

    //Main program entrance
    val flist = new File("resources/corrdir/").listFiles.filter(_.getName.endsWith(".fits"))

    val fitsList = (0 until flist.length).map(i => FitsUtils.readFits(flist(i).toString)).toArray

    val map = FitsUtils.processMeta(fitsList)

    val template = new Template("resources/template.hdr")

    val matrix = add(fitsList, map, template.tcol, template.trow)

    FitsUtils.createFits(template, matrix, "final.fits")
  }
}
