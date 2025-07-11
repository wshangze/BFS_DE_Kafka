package com.example;

public class Validator {
    public static boolean isValid(Employee e) {
        if (e == null) return false;
        if (e.emp_id <= 0) return false;
        if (!e.action.matches("INSERT|UPDATE|DELETE")) return false;
        if (!e.emp_dob.matches("\\d{4}-\\d{2}-\\d{2}")) return false;
        if (e.emp_FN == null || e.emp_FN.isEmpty()) return false;
        if (e.emp_LN == null || e.emp_LN.isEmpty()) return false;
        if (e.emp_city == null || e.emp_city.isEmpty()) return false;
        return true;
    }
}
