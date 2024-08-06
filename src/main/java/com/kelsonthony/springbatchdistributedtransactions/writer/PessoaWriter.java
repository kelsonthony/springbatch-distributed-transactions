package com.kelsonthony.springbatchdistributedtransactions.writer;

import com.kelsonthony.springbatchdistributedtransactions.model.Pessoa;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@Configuration
@EnableBatchProcessing
public class PessoaWriter {

    @Bean
    public ItemWriter<Pessoa> writer(
            @Qualifier("writerDb1") ItemWriter<Pessoa> writerDb1,
            @Qualifier("writerDb2") ItemWriter<Pessoa> writerDb2) {
        return new CompositeItemWriterBuilder<Pessoa>()
                .delegates(List.of(writerDb1, writerDb2))
                .build();
    }

    @Bean
    public ItemWriter<Pessoa> writerDb1(@Qualifier("db1DS") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Pessoa>()
                .dataSource(dataSource)
                .sql(
                        "INSERT INTO pessoa (id, nome, email, data_nascimento, idade) VALUES (:id, :nome, :email, :dataNascimento, :idade)")
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .build();
    }

    @Bean
    public ItemWriter<Pessoa> writerDb2(@Qualifier("db2DS") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Pessoa>()
                .dataSource(dataSource)
                .sql(
                        "INSERT INTO pessoa (id, nome, email, data_nascimento, idade) VALUES (?, ?, ?, ?, ?)")
                .itemPreparedStatementSetter(itemPreparedStatementSetter())
                .build();
    }

    // Para simular um erro no db2
    private ItemPreparedStatementSetter<Pessoa> itemPreparedStatementSetter() {
        return new ItemPreparedStatementSetter<Pessoa>() {

            @Override
            public void setValues(Pessoa pessoa, PreparedStatement ps) throws SQLException {
//                if (pessoa.getId() == 1071)
//                    throw new SQLException("Opa, deu erro!");

                ps.setInt(1, pessoa.getId());
                ps.setString(2, pessoa.getNome());
                ps.setString(3, pessoa.getEmail());
                ps.setString(4, pessoa.getDataNascimento());
                ps.setInt(5, pessoa.getIdade());
            }

        };
    }
}
