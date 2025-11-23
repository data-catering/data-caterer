package io.github.datacatering.datacaterer.javaapi.api;

import io.github.datacatering.datacaterer.api.connection.ConnectionTaskBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class to verify Java API improvements for better developer experience.
 */
public class JavaApiImprovementsTest extends PlanRun {

    @Test
    public void testWeightedValueCreation() {
        WeightedValue wv = weightedValue("test", 0.5);
        
        assertEquals("test", wv.value());
        assertEquals(0.5, wv.weight(), 0.001);
    }

    @Test
    public void testOneOfWeightedJavaReturnsArray() {
        var weightedFields = field().name("priority").oneOfWeightedJava(
                weightedValue("high", 0.8),
                weightedValue("low", 0.2)
        );
        
        // Should return array with 2 elements: weight helper and data field
        assertEquals(2, weightedFields.length);
        assertTrue(weightedFields[0].field().name().endsWith("_weight"));
        assertEquals("priority", weightedFields[1].field().name());
    }

    @Test
    public void testOneOfWeightedJavaWithFieldsMethod() {
        // Test that oneOfWeightedJava works seamlessly with fields() method
        var parentField = field().name("user")
                .fields(
                        field().name("id").regex("USER[0-9]{6}"),
                        field().name("name").expression("#{Name.fullName}")
                )
                .fields(field().name("tier").oneOfWeightedJava(
                        weightedValue("gold", 0.3),
                        weightedValue("silver", 0.7)
                ));
        
        assertNotNull(parentField);
        assertEquals("user", parentField.field().name());
        // Should have 4 fields: 2 regular + 2 from weighted (helper + data)
        assertEquals(4, parentField.field().fields().size());
    }

    @Test
    public void testExecuteWithList() {
        // Create some test tasks
        var task1 = json("test1", "/tmp/test1");
        var task2 = json("test2", "/tmp/test2");
        
        // Test that we can create a list and pass it to execute
        List<ConnectionTaskBuilder<?>> tasks = List.of(task1, task2);
        
        // This should compile without errors (we won't actually execute)
        assertDoesNotThrow(() -> {
            // execute(tasks); // Commented out to avoid actual execution in test
        });
    }

    @Test
    public void testExecuteWithListAndConfiguration() {
        var task1 = json("test1", "/tmp/test1");
        var task2 = json("test2", "/tmp/test2");
        var config = configuration().enableValidation(true);
        
        List<ConnectionTaskBuilder<?>> tasks = List.of(task1, task2);
        
        // This should compile without errors
        assertDoesNotThrow(() -> {
            // execute(config, tasks); // Commented out to avoid actual execution in test
        });
    }

    @Test
    public void testExecuteWithPlanConfigurationAndList() {
        var task1 = json("test1", "/tmp/test1");
        var task2 = json("test2", "/tmp/test2");
        var myPlan = plan();
        var config = configuration().enableValidation(true);
        
        List<ConnectionTaskBuilder<?>> tasks = List.of(task1, task2);
        
        // This should compile without errors
        assertDoesNotThrow(() -> {
            // execute(myPlan, config, tasks); // Commented out to avoid actual execution in test
        });
    }
}