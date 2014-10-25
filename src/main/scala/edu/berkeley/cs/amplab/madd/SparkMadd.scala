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
import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import scala.annotation.tailrec

object SparkMadd extends Serializable {
  def main(args: Array[String]) {

    def parallelize(f: Fits,
                    metadata: FitsMetadata,
                    sc: SparkContext): RDD[(Coordinate, Float)] = {
      sc.parallelize(f.matrix.zipWithIndex.flatMap(vk => {
        val (array, idx) = vk

        if (idx <= metadata.length) {
          array.zipWithIndex.flatMap(vk2 => {
            val (value, jdx) = vk2

            if (value.isNaN || value.isInfinite) {
              None
            } else {
              Some((Coordinate(idx + metadata.start, jdx + metadata.offset), value))
            }
          })
        } else {
          Iterable[(Coordinate, Float)]()
        }
      }))
    }

    val conf = new SparkConf().setAppName("SparkMadd")
    if (conf.getOption("spark.master").isEmpty) {
      conf.setMaster("local[%d]".format(Runtime.getRuntime.availableProcessors()))
    }
    val sc = new SparkContext(conf)

    @tailrec def buildUp(datasets: Iterator[(Fits, FitsMetadata)],
                         lastRdd: RDD[(Coordinate, Float)] = sc.parallelize(Array[(Coordinate, Float)]())): RDD[(Coordinate, Float)] = {
      if (!datasets.hasNext) {
        lastRdd
      } else {
        // cache last RDD
        lastRdd.cache()

        // parallelize the current dataset
        val (data, metadata) = datasets.next
        val newRdd = parallelize(data, metadata, sc) ++ lastRdd

        // unpersist the old rdd and recurse
        lastRdd.unpersist()
        buildUp(datasets, newRdd)
      }
    }

    def add(rdd: RDD[(Coordinate, Float)],
            tcol: Int,
            trow: Int): Array[Array[Float]] = {
      // create inital matrix
      val matrix = Array.fill[Float](tcol, trow)(Float.NaN)

      // populate matrix
      rdd.filter(kv => {
        val (idx, _) = kv
        idx.x < tcol && idx.y < trow
      }).groupByKey()
        .map(kv => {
          val (idx, values) = kv

          (idx, values.sum / values.size.toFloat)
        }).toLocalIterator
        .foreach(kv => {
          val (idx, value) = kv

          matrix(idx.x)(idx.y) = value
        })

      matrix
    }

    //Main program entrance
    val flist = new File("resources/corrdir/").listFiles.filter(_.getName.endsWith(".fits"))

    // get data and metadata
    val fitsList = (0 until flist.length).map(i => FitsUtils.readFits(flist(i).toString)).toArray
    val map = FitsUtils.processMeta(fitsList).map(kv => kv._2)

    // get template
    val template = new Template("resources/template.hdr")

    // parallelize files
    val fitsRdd = buildUp(fitsList.zip(map).toIterator)

    // reduce down to matrix
    val matrix = add(fitsRdd, template.tcol, template.trow)

    // save output
    FitsUtils.createFits(template, matrix, "final.fits")
  }
}
