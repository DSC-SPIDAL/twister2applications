//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.apps.mds;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.FSDataOutputStream;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.nio.channels.FileChannel;
import java.util.logging.Logger;

import static java.lang.Short.MAX_VALUE;

public class MatrixGenerator {

  private static final Logger LOG = Logger.getLogger(MatrixGenerator.class.getName());

  private Config config;

  private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;
  private int workerId;

  public MatrixGenerator(Config cfg, int workerid) {
    this.config = cfg;
    this.workerId = workerid;
  }

  public MatrixGenerator(Config cfg) {
    this.config = cfg;
  }

  /**
   * To generate the matrix for MDS application
   * @param matrixRowLength
   * @param matrixColumnLength
   * @param directory
   * @param byteType
   */
  public void generate(int matrixRowLength, int matrixColumnLength, String directory, String byteType) {
    endianness = "big".equals(byteType) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    short[] input = new short[matrixRowLength * matrixColumnLength];
    for (int i = 0; i < matrixRowLength * matrixColumnLength; i++) {
      double temp = Math.random() * MAX_VALUE;
      input[i] = (short) temp;
    }
    try {
      ByteBuffer byteBuffer = ByteBuffer.allocate(matrixRowLength * matrixColumnLength * 2);
      if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
        byteBuffer.order(ByteOrder.BIG_ENDIAN);
      } else {
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
      }

      byteBuffer.clear();
      ShortBuffer shortOutputBuffer = byteBuffer.asShortBuffer();
      shortOutputBuffer.put(input);

      Path pathDirectory = new Path(directory);
      FileSystem fs = FileSystemUtils.get(pathDirectory.toUri(), config);
      if (fs.exists(pathDirectory)) {
        if (!fs.delete(pathDirectory, true)) {
          throw new IOException("Failed to delete the directory: " + pathDirectory.getPath());
        }
      }
      FSDataOutputStream outputStream = fs.create(new Path(directory, generateRandom(10) + ".bin"));
      FileChannel out = outputStream.getChannel();
      out.write(byteBuffer);
      outputStream.flush();
      outputStream.sync();
      out.close();
    } catch (IOException e) {
      throw new RuntimeException("IOException Occured" + e.getMessage());
    }
  }

  private static String generateRandom(int length) {
    boolean useLetters = true;
    boolean useNumbers = false;
    return RandomStringUtils.random(length, useLetters, useNumbers);
  }
}
