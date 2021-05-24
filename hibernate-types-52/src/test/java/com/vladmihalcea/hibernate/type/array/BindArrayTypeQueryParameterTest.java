package com.vladmihalcea.hibernate.type.array;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.vladmihalcea.hibernate.type.util.AbstractPostgreSQLIntegrationTest;
import com.vladmihalcea.hibernate.type.util.ExceptionUtil;
import com.vladmihalcea.hibernate.type.util.providers.DataSourceProvider;
import com.vladmihalcea.hibernate.type.util.providers.PostgreSQLDataSourceProvider;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.ParameterExpression;
import javax.persistence.criteria.Root;
import javax.sql.DataSource;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;
import org.hibernate.jpa.TypedParameterValue;
import org.hibernate.query.Query;
import org.junit.Test;

/**
 * @author Stanislav Gubanov
 */
public class BindArrayTypeQueryParameterTest extends AbstractPostgreSQLIntegrationTest {

    @Override
    protected Class<?>[] entities() {
        return new Class<?>[]{
                Event.class, EventWithListValues.class
        };
    }

    @Override
    public void init() {
        DataSource dataSource = newDataSource();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                "CREATE OR REPLACE FUNCTION " +
                "    fn_array_contains(" +
                "       left_array integer[], " +
                "       right_array integer[]" +
                ") RETURNS " +
                "       boolean AS " +
                "$$ " +
                "BEGIN " +
                "  return left_array @> right_array; " +
                "END; " +
                "$$ LANGUAGE 'plpgsql';"
            );
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        super.init();
    }

    @Override
    protected DataSourceProvider dataSourceProvider() {
        return new PostgreSQLDataSourceProvider() {
            @Override
            public String hibernateDialect() {
                return PostgreSQL95ArrayDialect.class.getName();
            }
        };
    }

    @Override
    protected void afterInit() {
        doInJPA(entityManager -> {
            Event event = new Event();
            event.setId(1L);
            event.setName("Temperature");
            event.setValues(new int[]{1, 2, 3});
            entityManager.persist(event);
        });

        doInJPA(entityManager -> {
            EventWithListValues event = new EventWithListValues();
            event.setId(1L);
            event.setName("TemperatureList");
            event.setValues(List.of(1L, 2L, 3L));
            entityManager.persist(event);
        });
    }

    @Test
    public void testJPQLWithDefaultParameterBiding() {
        try {
            doInJPA(entityManager -> {
                Event event = entityManager
                .createQuery(
                    "select e " +
                    "from Event e " +
                    "where " +
                    "   fn_array_contains(e.values, :arrayValues) = true", Event.class)
                .setParameter("arrayValues", new int[]{2, 3})
                .getSingleResult();
            });
        } catch (Exception e) {
            Exception rootCause = ExceptionUtil.rootCause(e);
            assertTrue(rootCause.getMessage().contains("ERROR: function fn_array_contains(integer[], bytea) does not exist"));
        }
    }

    @Test
    public void testJPQLWithExplicitParameterTypeBinding() {
        doInJPA(entityManager -> {
            Event event = (Event) entityManager
            .createQuery(
                "select e " +
                "from Event e " +
                "where " +
                "   fn_array_contains(e.values, :arrayValues) = true", Event.class)
            .unwrap(org.hibernate.query.Query.class)
            .setParameter("arrayValues", new int[]{2, 3}, IntArrayType.INSTANCE)
            .getSingleResult();

            assertArrayEquals(new int[]{1, 2, 3}, event.getValues());
        });
    }

    @Test
    public void testJPQLWithTypedParameterValue() {
        doInJPA(entityManager -> {
            Event event = entityManager
            .createQuery(
                "select e " +
                "from Event e " +
                "where " +
                "   fn_array_contains(e.values, :arrayValues) = true", Event.class)
            .setParameter("arrayValues", new TypedParameterValue(IntArrayType.INSTANCE, new int[]{2, 3}))
            .getSingleResult();

            assertArrayEquals(new int[]{1, 2, 3}, event.getValues());
        });
    }

    @Test
    public void testCriteriaAPI() {
        doInJPA(entityManager -> {
            CriteriaBuilder cb = entityManager.getCriteriaBuilder();
            CriteriaQuery<Event> cq = cb.createQuery(Event.class);
            Root<Event> root = cq.from(Event.class);
            cq.select(root);

            ParameterExpression containValues = cb.parameter(int[].class, "arrayValues");
            cq.where(
                cb.equal(
                    cb.function(
                        "fn_array_contains",
                        Boolean.class,
                        root.get("values"), containValues
                    ),
                    Boolean.TRUE
                )
            );

            Event event = (Event) entityManager
            .createQuery(cq)
            .unwrap(Query.class)
            .setParameter("arrayValues", new int[]{2, 3}, IntArrayType.INSTANCE)
            .getSingleResult();

            assertArrayEquals(new int[]{1, 2, 3}, event.getValues());
        });
    }

    @Entity(name = "Event")
    @Table(name = "event")
    @TypeDef(
        typeClass = IntArrayType.class,
        defaultForType = int[].class
    )
    public static class Event {

        @Id
        private Long id;

        private String name;

        @Column(
            name = "event_values",
            columnDefinition = "integer[]"
        )
        private int[] values;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int[] getValues() {
            return values;
        }

        public void setValues(int[] values) {
            this.values = values;
        }
    }

    @Entity(name = "EventWithListValues")
    @Table(name = "event_with_list_values")
    @TypeDef(name = "list-array", typeClass = ListArrayType.class)
    public static class EventWithListValues {

        @Id
        private Long id;

        private String name;

        @Type(type = "list-array")
        @Column(
            name = "event_values",
            columnDefinition = "bigint[]"
        )
        private List<Long> values;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<Long> getValues() {
            return values;
        }

        public void setValues(List<Long> values) {
            this.values = values;
        }
    }


    @Test
    public void testQueryEventWithListValues() {
        final List<Long> param = Arrays.asList (1L, 2L, 3L);
        doInJPA(entityManager -> {
            // fails here when
            TypedQuery<EventWithListValues> cq = entityManager
                .createQuery(
                    "select e " +
                        "from EventWithListValues e " +
                        "where " +
                        "   e.values = :listValues", EventWithListValues.class);
            TypedQuery<EventWithListValues> pq = cq.setParameter("listValues", new TypedParameterValue(ListArrayType.INSTANCE, param));
            EventWithListValues event = pq.getSingleResult();
            assertEquals(event.getValues(), param);
        });
    }
}
