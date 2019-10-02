package com.trespark.elastic.controller;

import com.trespark.elastic.config.ElasticSearchConfig;
import com.trespark.elastic.dao.Customer;
import com.trespark.elastic.dao.CustomerDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/customers")
public class CustomerController {

	//@Autowired
    private CustomerDao customerDao;
	
	//@Autowired
	//private ElasticSearchConfig elastic;

	
    public CustomerController(CustomerDao customerDao) {
        this.customerDao = customerDao;
    } 

    @PostMapping
    public Customer insertCustomer(@RequestBody Customer customer) throws Exception{
    //	elastic.getMyClient();
    	System.out.println("Inside - insertCustomer" + customer.getName());
        return customerDao.insertCustomer(customer); 	
    }

    @GetMapping("/{id}")
    public Map<String, Object> getCustomerById(@PathVariable String id){
    	return customerDao.getCustomerById(id);     
    }

    @PutMapping("/{id}")
    public Map<String, Object> updateCustomerById(@RequestBody Customer customer, @PathVariable String id){
    	return customerDao.updateCustomerById(id, customer);       	
    }

    @DeleteMapping("/{id}")
    public void deleteCustomerById(@PathVariable String id){
    	customerDao.deleteCustomerById(id);         
    }
}
