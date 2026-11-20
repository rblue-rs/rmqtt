#define _POSIX_C_SOURCE 200809L
#include "mqtt_client.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
static void nap(void) {struct timespec ts={0,2000000};nanosleep(&ts,NULL);}
static MqttEvent wait_kind(uint64_t h,uint32_t kind) {
 MqttEvent e={0}; uint8_t topic[65535],payload[65536];
 for(int i=0;i<2500;i++) {int code=mqtt_client_poll(h,&e,topic,sizeof(topic),payload,sizeof(payload));if(code==10){nap();continue;}assert(code==0);assert(e.kind!=8);if(e.kind==kind)return e;}
 assert(!"Timed out waiting for event");return e;
}
static int owned_frees=0;
static void owned_free(void* p){owned_frees++;free(p);}
static void ownership_smoke(void){
 for(int i=0;i<2;i++){
  size_t owner=0,token=0;uint64_t h=0;
  assert(mqtt_dart_owner_new(&owner)==0);
  assert(mqtt_client_new((const uint8_t*)"localhost",9,(const uint8_t*)"owned",5,1883,&h)==0);
  MqttEvent* event=calloc(1,sizeof(*event));uint8_t* topic=calloc(65535,1);uint8_t* payload=calloc(65536,1);
  assert(event&&topic&&payload);
  assert(mqtt_dart_client_register(owner,h,event,topic,payload,owned_free,&token)==0);
  assert(mqtt_dart_owner_shutdown(owner)==4);
  if(i==0)mqtt_dart_owner_finalize((void*)(uintptr_t)owner);
  else mqtt_dart_client_finalize((void*)(uintptr_t)token);
  assert(mqtt_dart_client_release(token)==0);
  assert(mqtt_dart_owner_shutdown(owner)==0);
  mqtt_dart_owner_finalize((void*)(uintptr_t)owner);
  MqttEvent out={0};assert(mqtt_client_poll(h,&out,NULL,0,NULL,0)==3);
 }
 assert(owned_frees==6);assert(mqtt_runtime_shutdown()==0);
}
int main(void) {
 const char *port=getenv("MQTT_TEST_PORT");assert(port);uint64_t h=0;
 assert(mqtt_runtime_init()==0);
 assert(mqtt_client_new((const uint8_t*)"127.0.0.1",9,(const uint8_t*)"c-smoke",7,(uint16_t)atoi(port),&h)==0);
 MqttEvent untouched;memset(&untouched,0xa5,sizeof(untouched));
 MqttEvent snapshot;memcpy(&snapshot,&untouched,sizeof(snapshot));
 uint8_t empty_t[2]={0xaa,0xaa},empty_p[2]={0xbb,0xbb};
 assert(mqtt_client_poll(h,&untouched,empty_t,2,empty_p,2)==10);
 assert(memcmp(&untouched,&snapshot,sizeof(untouched))==0);
 assert(empty_t[0]==0xaa && empty_t[1]==0xaa && empty_p[0]==0xbb && empty_p[1]==0xbb);
 assert(mqtt_client_poll(h,NULL,NULL,0,NULL,0)==1);
 assert(mqtt_client_poll(h,&untouched,NULL,1,NULL,0)==1);
 assert(mqtt_client_publish(h,1,(const uint8_t*)"x",1,NULL,1,0,0)==1);
 assert(mqtt_client_connect(h,1,4,0,0,NULL,0,NULL,0,NULL,0,0,NULL,0,NULL,0,0,0,NULL,0,NULL,0)==0);
 MqttEvent e=wait_kind(h,1);assert(e.operation_id==1);assert(!e.session_present);
 assert(mqtt_client_subscribe(h,2,(const uint8_t*)"slice/c",7,1)==0);
 e=wait_kind(h,2);assert(e.operation_id==2);assert(e.qos==1);
 uint8_t original[]={0,255,7,0};
 assert(mqtt_client_publish(h,3,(const uint8_t*)"slice/c",7,original,sizeof(original),1,0)==0);
 memset(original,99,sizeof(original)); // Native copied all input before returning.
 int queued=0,sent=0,acked=0,message=0;
 for(int i=0;i<2500 && !(queued&&sent&&acked&&message);i++) {
  int code=mqtt_client_poll(h,&e,NULL,0,NULL,0);
  if(code==10){nap();continue;}
  if(code==9) {
   assert(e.kind==6);assert(e.topic_len==7);assert(e.payload_len==4);
   uint8_t t[9],p[6];memset(t,0xaa,sizeof(t));memset(p,0xbb,sizeof(p));
   assert(mqtt_client_poll(h,&e,t,7,p,3)==9);
   for(size_t j=0;j<sizeof(t);j++)assert(t[j]==0xaa);
   for(size_t j=0;j<sizeof(p);j++)assert(p[j]==0xbb);
   assert(mqtt_client_poll(h,&e,t,7,p,4)==0);
   assert(memcmp(t,"slice/c",7)==0);const uint8_t expected[]={0,255,7,0};assert(memcmp(p,expected,4)==0);
   assert(t[7]==0xaa && t[8]==0xaa && p[4]==0xbb && p[5]==0xbb);message=1;
  } else {
   assert(code==0);assert(e.operation_id==3);
   if(e.kind==3){assert(!sent);queued=1;}
   if(e.kind==4){assert(queued && !acked);sent=1;}
   if(e.kind==5){assert(sent);acked=1;}
  }
 }
 assert(queued&&sent&&acked&&message);
 assert(mqtt_client_disconnect(h,4)==0);e=wait_kind(h,7);assert(e.operation_id==4);
 assert(mqtt_client_connect(h,5,4,0,0,NULL,0,NULL,0,NULL,0,0,NULL,0,NULL,0,0,0,NULL,0,NULL,0)==0);wait_kind(h,1);
 assert(mqtt_client_close(h)==0);assert(mqtt_client_free(h)==0);assert(mqtt_client_free(h)==0);
 assert(mqtt_client_publish(h,6,(const uint8_t*)"x",1,NULL,0,0,0)==3);
 assert(mqtt_runtime_shutdown()==0);
 ownership_smoke();
 puts("C ABI PASS: owned-buffer once-only cleanup, lifecycle, CONNACK/SUBACK, input copies, QoS1 stages, binary buffers/retry, reconnect");
}
