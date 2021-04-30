package no.difi.statistics.ingest.validation;


import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class OrgnoValidator implements ConstraintValidator<ValidOrgno, String> {

    @Override
    public void initialize(ValidOrgno validOrgno) {

    }

    @Override
    public boolean isValid(String s, ConstraintValidatorContext constraintValidatorContext) {
        return s == null || isValid(s);
    }

    // copy form eid-common: OrgnrValidator.java
    private boolean isValid(String orgnr) {
        if (orgnr == null) {
            return false;
        }
        if (! orgnr.matches("\\d{9}")) {
            return false;
        }
        int sum = 0;
        int weight[] = {3, 2, 7, 6, 5, 4, 3, 2};
        try {
            for (int i = 0; i < 8; i++) {
                int t = Integer.valueOf(orgnr.substring(i, i + 1));
                sum += t * weight[i];
            }

            if ((11 - (sum % 11)) % 11 != Integer.valueOf(orgnr.substring(8, 9))) {
                return false;
            }

        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }


}
