package io.aadesh.betterreadsdataloder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import connection.DatastacksAstraProperties;
import io.aadesh.betterreadsdataloder.author.Author;
import io.aadesh.betterreadsdataloder.author.AuthorRepo;
import io.aadesh.betterreadsdataloder.book.Book;
import io.aadesh.betterreadsdataloder.book.BookRepo;

@SpringBootApplication
@EnableConfigurationProperties(DatastacksAstraProperties.class)
public class BetterReadsDataloderApplication {

	@Autowired
	AuthorRepo aurepo;

	@Autowired
	BookRepo brepo;

	@Value("${datadump.location.author}")
	private String authorLocation;

	@Value("${datadump.location.works}")
	private String worksLocation;

	public static void main(String[] args) {
		SpringApplication.run(BetterReadsDataloderApplication.class, args);
	}

	@PostConstruct
	public void start() {
		initauthors();
		initworks();
	}

	private void initauthors() {
		Path path = Paths.get(authorLocation);
		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				// read and parse lines
				String jsonString = line.substring(line.indexOf("{"));

				try {
					JSONObject jo = new JSONObject(jsonString);
					// construct author object
					Author author = new Author();
					author.setName(jo.optString("name"));
					author.setPresonalName(jo.optString("personal_name"));
					author.setId(jo.optString("key").replace("/authors/", ""));
					// save it to database

					aurepo.save(author);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				;

			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void initworks() {
		Path path = Paths.get(worksLocation);
		DateTimeFormatter dateTime = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				String jsonstring = line.substring(line.indexOf("{"));
				try {
					JSONObject jo = new JSONObject(jsonstring);
					Book book = new Book();
					book.setId(jo.getString("key").replace("/works/", ""));
					JSONObject descjo = jo.optJSONObject("description");
					if (descjo != null) {
						book.setDescription(descjo.optString("value"));
					}

					book.setName(jo.optString("title"));
					List<String> authorIds;
					JSONArray authorArray = jo.optJSONArray("authors");
					if (authorArray != null) {
						authorIds = new ArrayList<>();
						for (int i = 0; i < authorArray.length(); i++) {
							authorIds.add(authorArray.getJSONObject(i).getJSONObject("author").getString("key")
									.replace("/authors/", ""));
						}
						book.setAuthorIds(authorIds);

						List<String> authorNames = authorIds.stream().map(id -> aurepo.findById(id))
								.map(optionalAuthor -> {
									if (!optionalAuthor.isPresent())
										return "Unknown Author";
									return optionalAuthor.get().getName();
								}).collect(Collectors.toList());

						book.setCauthorNames(authorNames);
					}

					JSONArray coverJsonArray = jo.optJSONArray("covers");
					if (coverJsonArray != null) {
						List<String> coverIds = new ArrayList<>();
						for (int i = 0; i < coverJsonArray.length(); i++) {
							coverIds.add(coverJsonArray.getString(i));
						}
						book.setCoverIds(coverIds);
					}

					JSONObject dateob = jo.getJSONObject("created");
					if (dateob != null) {
						book.setPublishedDate(LocalDate.parse(dateob.optString("value"), dateTime));
					}
					brepo.save(book);

				} catch (JSONException e) {
					e.printStackTrace();
				}

			});

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DatastacksAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureconnectbundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

}
