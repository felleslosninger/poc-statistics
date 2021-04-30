package no.difi.statistics.ingest.validation;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.*;

@Documented
@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = {OrgnoValidator.class})
public @interface ValidOrgno {
    String message() default "{ValidOrgno}";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};
}
