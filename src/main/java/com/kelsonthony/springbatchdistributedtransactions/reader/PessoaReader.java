package com.kelsonthony.springbatchdistributedtransactions.reader;

import com.kelsonthony.springbatchdistributedtransactions.model.Pessoa;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

@Configuration
@EnableBatchProcessing
public class PessoaReader {

    @Bean
    public ItemReader<Pessoa> reader() {
        return new FlatFileItemReaderBuilder<Pessoa>()
                .name("reader")
                .resource(new FileSystemResource("files/pessoas.csv"))
                .comments("--")
                .delimited()
                .names("nome", "email", "dataNascimento", "idade", "id")
                .targetType(Pessoa.class)
                .build();
    }


}
