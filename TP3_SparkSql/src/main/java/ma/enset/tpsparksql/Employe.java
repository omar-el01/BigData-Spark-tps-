package ma.enset.tpsparksql;

import java.io.Serializable;

public class Employe implements Serializable {
    private long id;
    private String name;
    private String phone;
    private String departement;
    private long age;
    private double salary;

    public Employe() {
    }
    public Employe(long id, String name, String phone, String departement, long age, double salary) {
        this.id = id;
        this.name = name;
        this.phone = phone;
        this.departement = departement;
        this.age = age;
        this.salary = salary;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getPhone() {
        return phone;
    }

    public String getDepartement() {
        return departement;
    }

    public long getAge() {
        return age;
    }

    public double getSalary() {
        return salary;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public void setDepartement(String departement) {
        this.departement = departement;
    }

    public void setAge(long age) {
        this.age = age;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }
}
