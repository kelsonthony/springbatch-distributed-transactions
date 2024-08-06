package com.kelsonthony.springbatchdistributedtransactions.step;

import com.kelsonthony.springbatchdistributedtransactions.model.Pessoa;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class PessoaStep {

    private final StepBuilderFactory stepBuilderFactory;

    private final PlatformTransactionManager transactionManagerApp;

    public PessoaStep(StepBuilderFactory stepBuilderFactory,
                      @Qualifier("transactionManagerJta") PlatformTransactionManager transactionManagerApp) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.transactionManagerApp = transactionManagerApp;
    }

    @Bean
    public Step step(ItemReader<Pessoa> reader,
                     @Qualifier("writer") ItemWriter<Pessoa> writer) {
        return stepBuilderFactory
                .get("step")
                .<Pessoa, Pessoa>chunk(200)
                .reader(reader)
                .writer(writer)
                .transactionManager(transactionManagerApp)
                .build();
    }
}
