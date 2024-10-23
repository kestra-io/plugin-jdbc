package io.kestra.plugin.jdbc;

import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcQuery extends AbstractJdbcBaseQuery {

    public AbstractJdbcBaseQuery.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        AbstractCellConverter cellConverter = getCellConverter(this.zoneId());

        try (
            Connection conn = this.connection(runContext);
            Statement stmt = this.createStatement(conn)
        ) {
            if (this instanceof AutoCommitInterface autoCommitClass) {
                if (this.getFetchType().equals(FetchType.STORE)) {
                    conn.setAutoCommit(false);
                } else {
                    conn.setAutoCommit(autoCommitClass.getAutoCommit());
                }
            }

            stmt.setFetchSize(this.getFetchSize());

            String sql = runContext.render(this.sql, this.additionalVars);
            logger.debug("Starting query: {}", sql);

            boolean isResult = stmt.execute(sql);

            Output.OutputBuilder<?, ?> output = AbstractJdbcBaseQuery.Output.builder();
            long size = 0L;

            if (isResult) {
                try(ResultSet rs = stmt.getResultSet()) {
                    //Populate result fro result set
                    switch (this.getFetchType()) {
                        case FETCH_ONE -> {
                            size = 1L;
                            output
                                .row(fetchResult(rs, cellConverter, conn))
                                .size(size);
                        }
                        case STORE -> {
                            File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                            try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
                                size = fetchToFile(stmt, rs, fileWriter, cellConverter, conn);
                            }
                            output
                                .uri(runContext.storage().putFile(tempFile))
                                .size(size);
                        }
                        case FETCH -> {
                            List<Map<String, Object>> maps = new ArrayList<>();
                            size = fetchResults(stmt, rs, maps, cellConverter, conn);
                            output
                                .rows(maps)
                                .size(size);
                        }
                    }
                }
            }
            runContext.metric(Counter.of("fetch.size",  size, this.tags()));
            return output.build();
        }
    }
}
