package org.sid.demospringcloudstreamskafka.web;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.sid.demospringcloudstreamskafka.entities.PageEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.xml.crypto.Data;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RestController
public class PageEventRestController {
    @Autowired
    private StreamBridge streamBridge;
    @Autowired
    private InteractiveQueryService queryableStoreRegistry;
    @GetMapping("/publish/{topic}/{name}")
    public PageEvent publish(@PathVariable String topic, @PathVariable String name){
        PageEvent pageEvent = new PageEvent(name, Math.random()>0.5?"U1":"U2", new Date(), new Random().nextInt(9000));
        streamBridge.send(topic, pageEvent);
        return pageEvent;
    }

    @GetMapping("/counts")
    public Map<String, Long> counts(){
        Map<String, Long> results = new HashMap<>();
        ReadOnlyKeyValueStore<String, Long> queryableStoreType = this.queryableStoreRegistry.getQueryableStore("stats", QueryableStoreTypes.keyValueStore());
        KeyValueIterator<String, Long> all = queryableStoreType.all();
        while (all.hasNext()){
            KeyValue<String, Long> item = all.next();
            results.put(item.key, item.value);
        }
        return results;
    }
}
