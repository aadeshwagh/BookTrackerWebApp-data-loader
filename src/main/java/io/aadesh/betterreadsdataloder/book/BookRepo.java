package io.aadesh.betterreadsdataloder.book;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BookRepo extends CassandraRepository<Book, String> {

}
