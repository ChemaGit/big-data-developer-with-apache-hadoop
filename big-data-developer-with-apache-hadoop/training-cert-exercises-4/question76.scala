/** Question 76
 * Problem Scenario 38 : You have been given an RDD as below,
 * val rdd: RDD[Array[Byte]]
 * Now you have to save this RDD as a SequenceFile. And below is the code snippet.
 * import org.apache.hadoop.io.compress.GzipCodec
 * rdd.map(bytesArray => (A.get(), new B(bytesArray))).saveAsSequenceFile("/output/path",classOut[GzipCodec])
 * What would be the correct replacement for A and B in above snippet.
 */

Answer : See the explanation for Step by Step Solution and configuration.

Explanation: Solution : A. NullWritable B. BytesWritable

val rdd: RDD[Array[Byte]]
import org.apache.hadoop.io.compress.GzipCodec

rdd.map(bytesArray => (NullWritable.get(), new BytesWritable(bytesArray))).saveAsSequenceFile("/output/path",classOut[GzipCodec])