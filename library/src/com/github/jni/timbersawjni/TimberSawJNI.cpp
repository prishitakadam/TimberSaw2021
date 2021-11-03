
#include <jni.h>
#include <iostream>
#include "com_github_jni_timbersawjni_TimberSawJNI.h"

#include "TimberSaw/db.h"
#include "TimberSaw/cache.h"
#include "TimberSaw/filter_policy.h"
#include "TimberSaw/write_batch.h"

JNIEXPORT jlong JNICALL Java_LevelDb_open
  (JNIEnv* env, jobject thisObject, jstring fileName,
  jboolean createIfMissing, jboolean errorIfExists,
  jboolean compression, jboolean paranoidChecks,
  jlong cacheSizeBytes, jbyte bloomFilterBitsPerKey  ) {

   TimberSaw::Options options;
   options.create_if_missing = createIfMissing;
   options.error_if_exists = errorIfExists;
   if (!compression) {
        options.compression = TimberSaw::kNoCompression;
   }
   if (cacheSizeBytes>0) {
        options.block_cache = TimberSaw::NewLRUCache((size_t)cacheSizeBytes);
   }
   if (bloomFilterBitsPerKey>0) {
        options.filter_policy = TimberSaw::NewBloomFilterPolicy(bloomFilterBitsPerKey);
   }
   options.paranoid_checks = paranoidChecks;
   const char* fileNameCpp = env->GetStringUTFChars(fileName, NULL);
   TimberSaw::DB* db;
   TimberSaw::Status status = TimberSaw::DB::Open(options, fileNameCpp, &db);
   if (status.ok()) {
        return (jlong) db;
   }
   return (jlong) 0;
}

JNIEXPORT jboolean JNICALL Java_TimberSawJNI_put
  (JNIEnv* env, jobject thisObject, jlong dbRef, jbyteArray key, jbyteArray value) {
   jbyte *ptr = env->GetByteArrayElements(key, 0);
   std::string keyStr((char *)ptr, env->GetArrayLength(key));
   env->ReleaseByteArrayElements(key, ptr, 0);

   jbyte *ptrV = env->GetByteArrayElements(value, 0);
   std::string valueStr((char *)ptrV, env->GetArrayLength(value));
   env->ReleaseByteArrayElements(value, ptrV, 0);

   TimberSaw::Status status = ((TimberSaw::DB*)dbRef)->Put(TimberSaw::WriteOptions(), keyStr, valueStr);
   return status.ok();
}

JNIEXPORT jboolean JNICALL Java_TimberSawJNI_delete
  (JNIEnv* env, jobject thisObject, jlong dbRef, jbyteArray key) {
   jbyte *ptr = env->GetByteArrayElements(key, 0);
   std::string keyStr((char *)ptr, env->GetArrayLength(key));
   env->ReleaseByteArrayElements(key, ptr, 0);

   std::string value;
   TimberSaw::Status status = ((TimberSaw::DB*)dbRef)->Delete(TimberSaw::WriteOptions(), keyStr);
   return status.ok();
}

JNIEXPORT jbyteArray JNICALL Java_TimberSawJNI_get
  (JNIEnv* env, jobject thisObject, jlong dbRef, jbyteArray key) {
   jbyte *ptr = env->GetByteArrayElements(key, 0);
   std::string keyStr((char *)ptr, env->GetArrayLength(key));
   env->ReleaseByteArrayElements(key, ptr, 0);

   std::string value;
   TimberSaw::Status status = ((TimberSaw::DB*)dbRef)->Get(TimberSaw::ReadOptions(), keyStr, &value);
   if(!status.ok()) {
    return (jbyteArray) 0;
   }

   jbyteArray result = env->NewByteArray(value.length());
   env->SetByteArrayRegion(result,0,value.length(),(jbyte*)value.c_str());
   return result;
}

JNIEXPORT jlong JNICALL Java_TimberSawJNI_writeBatchNew
  (JNIEnv *, jobject thisObject, jlong dbRef) {
    if (!dbRef) {
        return (jlong) 0;
    }
    TimberSaw::WriteBatch* batch = new TimberSaw::WriteBatch();
    return (jlong) batch;
}

JNIEXPORT void JNICALL Java_TimberSawJNI_writeBatchPut
  (JNIEnv* env, jobject thisObject, jlong ref, jbyteArray key, jbyteArray value) {
   jbyte *ptr = env->GetByteArrayElements(key, 0);
   std::string keyStr((char *)ptr, env->GetArrayLength(key));
   env->ReleaseByteArrayElements(key, ptr, 0);

   jbyte *ptrV = env->GetByteArrayElements(value, 0);
   std::string valueStr((char *)ptrV, env->GetArrayLength(value));
   env->ReleaseByteArrayElements(value, ptrV, 0);

   ((TimberSaw::WriteBatch*)ref)->Put(keyStr, valueStr);
}

JNIEXPORT void JNICALL Java_TimberSawJNI_writeBatchDelete
  (JNIEnv* env, jobject thisObject, jlong ref, jbyteArray key) {
   jbyte *ptr = env->GetByteArrayElements(key, 0);
   std::string keyStr((char *)ptr, env->GetArrayLength(key));
   env->ReleaseByteArrayElements(key, ptr, 0);

   ((TimberSaw::WriteBatch*)ref)->Delete(keyStr);
}

JNIEXPORT jlong JNICALL Java_TimberSawJNI_iteratorNew
  (JNIEnv *, jobject thisObject, jlong dbRef) {
    if (!dbRef) {
        return (jlong) 0;
    }
    TimberSaw::Iterator* it = ((TimberSaw::DB*)dbRef)->NewIterator(TimberSaw::ReadOptions());
    return (jlong) it;
}

JNIEXPORT void JNICALL Java_TimberSawJNI_iteratorSeekToFirst
  (JNIEnv *, jobject, jlong ref) {
    if (ref) {
         ((TimberSaw::Iterator*)ref)->SeekToFirst();
    }
}

JNIEXPORT void JNICALL Java_TimberSawJNI_iteratorSeekToLast
  (JNIEnv *, jobject, jlong ref) {
    if (ref) {
         ((TimberSaw::Iterator*)ref)->SeekToLast();
    }
}

JNIEXPORT void JNICALL Java_TimberSawJNI_iteratorSeek
  (JNIEnv * env, jobject, jlong ref, jbyteArray key) {
    if (ref) {
        jbyte *ptr = env->GetByteArrayElements(key, 0);
        std::string keyStr((char *)ptr, env->GetArrayLength(key));
        env->ReleaseByteArrayElements(key, ptr, 0);
        ((TimberSaw::Iterator*)ref)->Seek(keyStr);
    }
}

JNIEXPORT void JNICALL Java_TimberSawJNI_iteratorNext
  (JNIEnv *, jobject, jlong ref) {
    if (ref) {
         ((TimberSaw::Iterator*)ref)->Next();
    }
}

JNIEXPORT void JNICALL Java_TimberSawJNI_iteratorPrev
  (JNIEnv *, jobject, jlong ref) {
    if (ref) {
         ((TimberSaw::Iterator*)ref)->Prev();
    }
}

JNIEXPORT jboolean JNICALL Java_TimberSawJNI_iteratorValid
  (JNIEnv *, jobject, jlong ref) {
    if (ref) {
         return ((TimberSaw::Iterator*)ref)->Valid();
    }
    return false;
}

JNIEXPORT jbyteArray JNICALL Java_TimberSawJNI_iteratorKey
  (JNIEnv* env, jobject thisObject, jlong ref) {

   std::string value;
   value = ((TimberSaw::Iterator*)ref)->key().ToString();

   jbyteArray result = env->NewByteArray(value.length());
   env->SetByteArrayRegion(result,0,value.length(),(jbyte*)value.c_str());
   return result;
}

JNIEXPORT jbyteArray JNICALL Java_TimberSawJNI_iteratorValue
  (JNIEnv* env, jobject thisObject, jlong ref) {

   std::string value;
   value = ((TimberSaw::Iterator*)ref)->value().ToString();

   jbyteArray result = env->NewByteArray(value.length());
   env->SetByteArrayRegion(result,0,value.length(),(jbyte*)value.c_str());
   return result;
}

JNIEXPORT jboolean JNICALL Java_TimberSawJNI_writeBatchWriteAndClose
  (JNIEnv *, jobject, jlong dbRef, jlong ref) {
    jboolean result = false;
    if (ref) {
        if (dbRef) {
            TimberSaw::Status status = ((TimberSaw::DB*)dbRef)->Write(TimberSaw::WriteOptions(),
                (TimberSaw::WriteBatch*) ref);
            result = status.ok();
        }
        delete (TimberSaw::WriteBatch*) ref;
    }
    return result;
}

JNIEXPORT void JNICALL Java_TimberSawJNI_iteratorClose
  (JNIEnv *, jobject, jlong ref) {
    if (ref) {
        delete (TimberSaw::Iterator*) ref;
    }
}

JNIEXPORT void JNICALL Java_TimberSawJNI_close
  (JNIEnv*, jobject, jlong dbRef) {
    if (dbRef) {
        delete (TimberSaw::DB*) dbRef;
    }
}
