package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;
import com.epam.rd.autocode.domain.Employee;
import com.epam.rd.autocode.domain.FullName;
import com.epam.rd.autocode.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DaoFactory {
    private static ConnectionSource connectionSource;

    public static void initConnectionSource() {
        connectionSource = ConnectionSource.instance();
    }

    private static Connection getConnection() throws SQLException {
        if (connectionSource == null) {
            initConnectionSource();
        }
        return connectionSource.createConnection();
    }

    public EmployeeDao employeeDAO() {
        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                try (Connection connection = getConnection();
                     PreparedStatement statement = connection.prepareStatement("SELECT * FROM EMPLOYEE WHERE DEPARTMENT = ?")) {
                    statement.setObject(1, department.getId());
                    try (ResultSet rs = statement.executeQuery()) {
                        List<Employee> employees = new ArrayList<>();
                        while (rs.next()) {
                            employees.add(mapToEmployee(rs));
                        }
                        return employees;
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                try (Connection connection = getConnection();
                     PreparedStatement statement = connection.prepareStatement("SELECT * FROM EMPLOYEE WHERE MANAGER = ?")) {
                    statement.setObject(1, employee.getId());
                    try (ResultSet rs = statement.executeQuery()) {
                        List<Employee> employees = new ArrayList<>();
                        while (rs.next()) {
                            employees.add(mapToEmployee(rs));
                        }
                        return employees;
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            private Employee mapToEmployee(ResultSet rs) throws SQLException {

                BigInteger id = BigInteger.valueOf(rs.getInt("id"));
                LocalDate hired = rs.getDate("HIREDATE").toLocalDate();
                BigDecimal salary = rs.getBigDecimal("SALARY");
                BigInteger managerId = BigInteger.valueOf(rs.getInt("MANAGER"));
                BigInteger departmentId = BigInteger.valueOf(rs.getInt("DEPARTMENT"));
                FullName fullName = new FullName(
                        rs.getString("FIRSTNAME"),
                        rs.getString("LASTNAME"),
                        rs.getString("MIDDLENAME")
                );
                Position position = Position.valueOf(rs.getString("POSITION"));

                Employee employee = new Employee(id, fullName, position, hired, salary, managerId, departmentId);

                return employee;
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                try (Connection connection = getConnection();
                     PreparedStatement statement = connection.prepareStatement("SELECT * FROM EMPLOYEE WHERE ID = ?")) {
                    statement.setObject(1, Id);
                    try (ResultSet rs = statement.executeQuery()) {
                        if (rs.next()) {
                            return Optional.of(mapToEmployee(rs));
                        } else {
                            return Optional.empty();
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public List<Employee> getAll() {
                try (Connection conn = getConnection();
                     Statement statement = conn.createStatement();
                     ResultSet rs = statement.executeQuery("SELECT * FROM EMPLOYEE")) {
                    List<Employee> employees = new ArrayList<>();
                    while (rs.next()) {
                        employees.add(mapToEmployee(rs));
                    }
                    return employees;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Employee save(Employee employee) {
                String sql = "INSERT INTO EMPLOYEE (ID, FIRSTNAME, LASTNAME, MIDDLENAME, POSITION, HIREDATE, SALARY, MANAGER, DEPARTMENT) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
                try (Connection connection = getConnection();
                     PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setObject(1, employee.getId());
                    statement.setString(2, employee.getFullName().getFirstName());
                    statement.setString(3, employee.getFullName().getLastName());
                    statement.setString(4, employee.getFullName().getMiddleName());
                    statement.setString(5, employee.getPosition().name());
                    statement.setDate(6, java.sql.Date.valueOf(employee.getHired()));
                    statement.setBigDecimal(7, employee.getSalary());
                    statement.setObject(8, employee.getManagerId());
                    statement.setObject(9, employee.getDepartmentId());
                    statement.executeUpdate();

                    return employee;

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void delete(Employee employee) {
                String sql = "DELETE FROM EMPLOYEE WHERE ID = ?";
                try (Connection connection = getConnection();
                     PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setObject(1, employee.getId());
                    statement.executeUpdate();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private Department mapToDeparment(ResultSet rs) throws SQLException {

        BigInteger id = BigInteger.valueOf(rs.getInt("id"));
        String name = rs.getString("NAME");
        String location = rs.getString("LOCATION");

        return new Department(id, name, location);
    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDao(){

            @Override
            public Optional<Department> getById(BigInteger id) {
                final String sql = "SELECT * FROM DEPARTMENT WHERE ID = ?";
                try (Connection connection = getConnection();
                     PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setObject(1, id);
                    ResultSet rs = statement.executeQuery();
                    if (rs.next()) {
                        return Optional.of(mapToDeparment(rs));
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                return Optional.empty();
            }

            @Override
            public List<Department> getAll() {
                final String sql = "SELECT * FROM DEPARTMENT";
                try (Connection connection = getConnection();
                     PreparedStatement statement = connection.prepareStatement(sql)) {
                    ResultSet rs = statement.executeQuery();
                    List<Department> departments = new ArrayList<>();
                    while (rs.next()) {
                        departments.add(mapToDeparment(rs));
                    }
                    return departments;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Department save(Department department) {
                Optional<Department> existingDepartment = getById(department.getId());

                if (existingDepartment.isPresent()) {
                    String sql = "UPDATE DEPARTMENT SET NAME = ?, LOCATION = ? WHERE ID = ?";
                    try (Connection connection = getConnection();
                         PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setString(1, department.getName());
                        statement.setString(2, department.getLocation());
                        statement.setObject(3, department.getId());
                        statement.executeUpdate();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    String sql = "INSERT INTO DEPARTMENT (ID, NAME, LOCATION) VALUES (?, ?, ?)";
                    try (Connection connection = getConnection();
                         PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setObject(1, department.getId());
                        statement.setString(2, department.getName());
                        statement.setString(3, department.getLocation());
                        statement.executeUpdate();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }

                return department;
            }


            @Override
            public void delete(Department department) {
                final String sql = "DELETE FROM DEPARTMENT WHERE ID = ?";
                try (Connection connection = getConnection();
                     PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setObject(1, department.getId());
                    statement.executeUpdate();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

        };
    }
}
