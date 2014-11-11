/*******************************************************************************
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use these files except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright 2014 - Juan Pino, Aurelien Waite, William Byrne
 *******************************************************************************/
/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.util;

import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.JCommander;

/**
 * Set of utilities. Static methods.
 * 
 * @author Aurelien Waite
 * @author Juan Pino
 * @date 28 May 2014
 */
public final class Util {

	private Util() {

	}

	/**
	 * Private recursive helper function to set properties based on JCommander 
	 * parameter objects
	 * @param params
	 * @param conf
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	private static void setProps(Object params, Configuration conf)
			throws IllegalArgumentException, IllegalAccessException {
		for (Field field : params.getClass().getDeclaredFields()) {
			String name = field.getName();
			Class<?> clazz = field.getType();
			Object val = field.get(params);
			if (Integer.class == clazz){
				conf.setInt(name, (Integer) val);
			}else if(Boolean.class == clazz){
				conf.setBoolean(name, (Boolean)val);
			}else if (String.class == clazz){
				conf.set(name, (String) val);
			}else{
				setProps(val, conf);
			}
		}
	}

	public static void ApplyConf(JCommander cmd, Configuration conf)
			throws IllegalArgumentException, IllegalAccessException {
		Object params = cmd.getObjects().get(0);
		ApplyConf(params, conf);
	}
	
	public static void ApplyConf(Object params, Configuration conf)
			throws IllegalArgumentException, IllegalAccessException {
		setProps(params, conf);
	}

}
