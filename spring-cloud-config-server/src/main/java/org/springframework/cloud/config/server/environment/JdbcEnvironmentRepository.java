/*
 * Copyright 2016-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cloud.config.server.environment;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.springframework.cloud.config.environment.Environment;
import org.springframework.cloud.config.environment.PropertySource;
import org.springframework.core.Ordered;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.util.StringUtils;
import org.yaml.snakeyaml.Yaml;

/**
 * An {@link EnvironmentRepository} that picks up data from a relational database. The
 * database should have a table called "PROPERTIES" with columns "APPLICATION", "PROFILE",
 * "LABEL" (with the usual {@link Environment} meaning), plus "KEY" and "VALUE" for the
 * key and value pairs in {@link Properties} style. Property values behave in the same way
 * as they would if they came from Spring Boot properties files named
 * <code>{application}-{profile}.properties</code>, including all the encryption and
 * decryption, which will be applied as post-processing steps (i.e. not in this repository
 * directly).
 * 
 * @author Dave Syer
 *
 */
public class JdbcEnvironmentRepository implements EnvironmentRepository, Ordered {
	private int order;
	private final JdbcTemplate jdbc;
	private String sql;
	private final PropertiesResultSetExtractor extractor = new PropertiesResultSetExtractor();

	public JdbcEnvironmentRepository(JdbcTemplate jdbc, JdbcEnvironmentProperties properties) {
		this.jdbc = jdbc;
		this.order = properties.getOrder();
		this.sql = properties.getSql();
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getSql() {
		return this.sql;
	}

	@Override
	public Environment findOne(String application, String profile, String label) {
		String config = application;
		if (StringUtils.isEmpty(label)) {
			label = "master";
		}
		if (StringUtils.isEmpty(profile)) {
			profile = "default";
		}
		if (!profile.startsWith("default")) {
			profile = "default," + profile;
		}
		String[] profiles = StringUtils.commaDelimitedListToStringArray(profile);
		Environment environment = new Environment(application, profiles, label, null,
				null);
		if (!config.startsWith("application")) {
			config = "application," + config;
		}
		List<String> applications = new ArrayList<String>(new LinkedHashSet<>(
				Arrays.asList(StringUtils.commaDelimitedListToStringArray(config))));
		List<String> envs = new ArrayList<String>(new LinkedHashSet<>(Arrays.asList(profiles)));
		Collections.reverse(applications);
		Collections.reverse(envs);
		//源版代码
//		for (String app : applications) {
//			for (String env : envs) {
//				Map<String, String> next = (Map<String, String>) jdbc.query(this.sql,
//						new Object[] { app, env, label }, this.extractor);
//				if (!next.isEmpty()) {
//					environment.add(new PropertySource(app + "-" + env, next));
//				}
//			}
//		}
		//修改代码 同时支持yml ，properties,kv多种配置
		for (String app : applications) {
			for (String env : envs) {
				Map<String, String> next = (Map<String, String>) jdbc.query(this.sql,
						new Object[] { app, env, label }, this.extractor);
				if (!next.isEmpty()) {
					next.entrySet().forEach(e->{
						String k = e.getKey();
						String v = e.getValue();
						if (k.endsWith(".yml")){
							try {
								Map<Object,Object> map = ymlToProperties(v);
								environment.add(new PropertySource(app + "-" + env, map));
							} catch (InterruptedException e1) {
								e1.printStackTrace();
							}
						}else if (k.endsWith(".properties")){
							try {
								InputStream in = new ByteArrayInputStream(v.getBytes());
								Properties prop = new Properties();
								prop.load(in);
								Map<Object,Object> pm = new HashMap<>((Map) prop);
								environment.add(new PropertySource(app + "-" + env, pm));
							} catch (IOException ioe) {
								ioe.printStackTrace();
							}
						}else {
							environment.add(new PropertySource(app + "-" + env, next));
						}
					});
				}
			}
		}
		return environment;
	}

	@Override
	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}


	public Map<Object,Object> ymlToProperties(String v) throws InterruptedException {
		Map<Object, Object> ym = new Yaml().loadAs(v, Map.class);
		BlockingQueue<MM> queue = new ArrayBlockingQueue(10);
		queue.put(new MM(null,ym));
		Map<Object,Object> rs = new HashMap<>();
		while (!queue.isEmpty()){
			MM mm = queue.poll();
			mm.map.entrySet().forEach(e->{
				if (e.getValue() instanceof Map){
					try {
						String k ="";
						if (mm.key!=null){
							k = mm.key+"."+e.getKey().toString();
						}else {
							k = e.getKey().toString();
						}
						queue.put(new MM(k,(Map<Object, Object>)e.getValue()));
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				}else {
					String k ="";
					if (mm.key!=null){
						k=mm.key+"."+e.getKey().toString();
					}else {
						k= e.getKey().toString();
					}
					rs.put(k,e.getValue().toString());
				}
			});
		}
		return rs;
	}

}
class MM{
	String key;
	Map<Object,Object> map;
	public MM(String key,Map<Object,Object> map){
		this.key=key;
		this.map=map;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public Map<Object, Object> getMap() {
		return map;
	}
	public void setMap(Map<Object, Object> map) {
		this.map = map;
	}
}


class PropertiesResultSetExtractor implements ResultSetExtractor<Map<String, String>> {

	@Override
	public Map<String, String> extractData(ResultSet rs)
			throws SQLException, DataAccessException {
		Map<String, String> map = new LinkedHashMap<>();
		while (rs.next()) {
			map.put(rs.getString(1), rs.getString(2));
		}
		return map;
	}

}