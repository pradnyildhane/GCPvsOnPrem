package com.example.GCPVsOPrem;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@RestController
@CrossOrigin
@EnableAutoConfiguration
@SpringBootApplication
public class GcpVsOPremApplication {

	@Autowired
	RestTemplate restTemplate;

	@Autowired
	JdbcTemplate jdbcTemplate;

	@RequestMapping("/load100") public String load100() throws IOException{

		String jdbcUrl = "jdbc:oracle:thin:@LNAC31U.uk.db.com:1725/LNAC31U.uk.db.com";
		String username="TA_TELEMETRY_OWNER";
		String password= "TA_T3L3M3TRY_OWN3R1-";

		File file = new ClassPathResource("files/load100.csv").getFile();

		int batchSize = 20;

		Connection connection = null;

		try{
			connection = DriverManager.getConnection(jdbcUrl,username,password);
			connection.setAutoCommit(false);

			String sql = "INSERT INTO movies ( title, description) VALUES (?,?)";
			PreparedStatement statement = connection.prepareStatement(sql);

			BufferedReader lineReader = new BufferedReader(new FileReader(file));

			String lineText = null;

			int count = 0;
			lineReader.readLine();

			while ((lineText=lineReader.readLine())!= null){
				String[] data = lineText.split(",");
				String title = data[0];
				String description = data[1];

				statement.setString(1,title);
				statement.setString(2,description);

				statement.addBatch();

				if(count%batchSize==0){
					statement.executeBatch();
				}
			}

			lineReader.close();

			statement.executeBatch();

			connection.commit();
			connection.close();

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return null;
	}

	public static void main(String[] args) {
		SpringApplication.run(GcpVsOPremApplication.class, args);
	}

}
