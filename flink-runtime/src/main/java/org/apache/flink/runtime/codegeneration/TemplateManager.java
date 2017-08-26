/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.codegeneration;

import org.apache.flink.util.FileUtils;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;

/**
 * {@link TemplateManager} is a singleton class that provides template rendering functionalities for code generation.
 * Such functionalities are caching, writing generated code to a file.
 */
public class TemplateManager {
	// ------------------------------------------------------------------------
	//                                   Constants
	// ------------------------------------------------------------------------
	public static final String TEMPLATE_ENCODING  = "UTF-8";

	private static final Logger LOG = LoggerFactory.getLogger(TemplateManager.class);

	// ------------------------------------------------------------------------
	//                                   Singleton Attribute
	// ------------------------------------------------------------------------
	private static TemplateManager templateManager;

	// ------------------------------------------------------------------------
	//                                   Attributes
	// ------------------------------------------------------------------------
	private final Configuration templateConf;
	private final String dirForGeneratedCode;

	/**
	 * Constructor.
	 * @throws IOException
	 */
	public TemplateManager(String generatedCodeDir) throws IOException {
		templateConf = new Configuration();
		templateConf.setClassForTemplateLoading(TemplateManager.class, "/templates");
		templateConf.setDefaultEncoding(TEMPLATE_ENCODING);
		templateConf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

		dirForGeneratedCode = prepareDirectoryGeneratedCode(generatedCodeDir);
	}


	/**
	 * A method to get a singleton instance
	 * or create one if it has been created yet.
	 * @return
	 * @throws IOException
	 */
	public static TemplateManager getInstance(String temporaryDir) throws IOException {
		if (templateManager == null){
			synchronized (TemplateManager.class){
				templateManager = new TemplateManager(temporaryDir);
			}
		}

		return templateManager;
	}


	/**
	 * Render sorter template with generated code provided by SorterTemplateModel and write the content to a file
	 * and cache the result for later calls.
	 * @param sorterTemplateModel
	 * @return name of the generated sorter
	 * @throws IOException
	 * @throws TemplateException
	 */
	public String getGeneratedCode(SorterTemplateModel model) throws IOException, TemplateException {

		Template template = templateConf.getTemplate(model.TEMPLATE_NAME);

		String generatedFilename = model.getSorterName();

		synchronized (this){

			FileOutputStream fs = new FileOutputStream(this.getPathToGeneratedCode(generatedFilename));

			Writer output = new OutputStreamWriter(fs);
			Map templateVariables = model.getTemplateVariables();
			template.process(templateVariables, output);

			fs.close();
			output.close();
		}

		return generatedFilename;
	}

	/**
	 * Prepare directory for storing generated code.
	 * @return path of the directory
	 */
	private String prepareDirectoryGeneratedCode(String generatedCodeDir) throws IOException {

		File dirForGeneratedCode = new File(generatedCodeDir + "/flink-codegeneration");

		if (!dirForGeneratedCode.exists()){
			LOG.debug("Creating diretory for generated code at : " + dirForGeneratedCode.getAbsolutePath());
			boolean res = dirForGeneratedCode.mkdir();
			if (!res){
				throw new IOException("Can't create temporary directory for generated code : " + dirForGeneratedCode.getAbsolutePath());
			}
		} else {
			LOG.debug(dirForGeneratedCode.getAbsolutePath() + " already exists, so just clean it up");
			FileUtils.cleanDirectory(dirForGeneratedCode);
		}

		return dirForGeneratedCode.getAbsolutePath();
	}

	/**
	 * Utility method to get the File object of the given filename
	 * return an object of File of the given file.
	 */
	public File getPathToGeneratedCode(String filename){
		return new File(this.dirForGeneratedCode + "/" + filename + ".java");
	}
}
