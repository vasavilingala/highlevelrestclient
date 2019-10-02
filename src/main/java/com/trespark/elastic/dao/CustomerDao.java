package com.trespark.elastic.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Repository
public class CustomerDao {

    private final String INDEX = "customerdata";
    private final String TYPE = "customers";

    
    private RestHighLevelClient restHighLevelClient;

    private ObjectMapper objectMapper;

    
    public CustomerDao( ObjectMapper objectMapper, RestHighLevelClient restHighLevelClient) {
        this.objectMapper = objectMapper;
        this.restHighLevelClient = restHighLevelClient;
    }
     
    public void setHighLevelClient(RestHighLevelClient restHighLevelClient) {
    	
    	this.restHighLevelClient = restHighLevelClient;
    }
    
    public Customer insertCustomer(Customer customer){
    	System.out.println(" This is a random UUID " + UUID.randomUUID().toString());
        customer.setId(UUID.randomUUID().toString());
        Map<String, Object> dataMap = objectMapper.convertValue(customer, Map.class);
        IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, customer.getId())
               .source(dataMap);
               
      
     /*   IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, customer.getId())
                .source("id", customer.getId(),
                        "name", customer.getName(),
                        "phone", customer.getPhone(),
                        "address", customer.getAddress());    
         */                
        try {
            IndexResponse response = restHighLevelClient.index(indexRequest);
        } catch(ElasticsearchException e) {
            e.getDetailedMessage();
        } catch (java.io.IOException ex){
            ex.getLocalizedMessage();
        }
        return customer;
      
    }

    public Map<String, Object> getCustomerById(String id){
        GetRequest getRequest = new GetRequest(INDEX, TYPE, id);
        GetResponse getResponse = null;
        try {
            getResponse = restHighLevelClient.get(getRequest);
            System.out.println(getResponse);
        } catch (java.io.IOException e){
            e.getLocalizedMessage();
        }
        Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
        return sourceAsMap;
    }
    
    
    public Map<String, Object> updateCustomerById(String id, Customer customer){
        UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, id)
                .fetchSource(true);    
        Map<String, Object> error = new HashMap<>();
        try {
            String customerJson = objectMapper.writeValueAsString(customer);
            updateRequest.doc(customerJson, XContentType.JSON);
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest);
            Map<String, Object> sourceAsMap = updateResponse.getGetResult().sourceAsMap();
            return sourceAsMap;
        }catch (JsonProcessingException e){
            e.getMessage();
        } catch (java.io.IOException e){
            e.getLocalizedMessage();
        }
        return error; 
    } 

    public void deleteCustomerById(String id) {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX, TYPE, id);
        try {
            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest);
        } catch (java.io.IOException e){
            e.getLocalizedMessage();
        }
        
    }

}
