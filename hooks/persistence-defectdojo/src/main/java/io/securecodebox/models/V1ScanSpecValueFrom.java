/*
 * Kubernetes
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * The version of the OpenAPI document: v1.18.2
 * 
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */


package io.securecodebox.models;

import java.util.Objects;
import java.util.Arrays;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.securecodebox.models.V1ScanSpecValueFromConfigMapKeyRef;
import io.securecodebox.models.V1ScanSpecValueFromFieldRef;
import io.securecodebox.models.V1ScanSpecValueFromResourceFieldRef;
import io.securecodebox.models.V1ScanSpecValueFromSecretKeyRef;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.IOException;

/**
 * Source for the environment variable&#39;s value. Cannot be used if value is not empty.
 */
@ApiModel(description = "Source for the environment variable's value. Cannot be used if value is not empty.")
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2020-10-21T08:16:15.156Z[Etc/UTC]")
public class V1ScanSpecValueFrom {
  public static final String SERIALIZED_NAME_CONFIG_MAP_KEY_REF = "configMapKeyRef";
  @SerializedName(SERIALIZED_NAME_CONFIG_MAP_KEY_REF)
  private V1ScanSpecValueFromConfigMapKeyRef configMapKeyRef;

  public static final String SERIALIZED_NAME_FIELD_REF = "fieldRef";
  @SerializedName(SERIALIZED_NAME_FIELD_REF)
  private V1ScanSpecValueFromFieldRef fieldRef;

  public static final String SERIALIZED_NAME_RESOURCE_FIELD_REF = "resourceFieldRef";
  @SerializedName(SERIALIZED_NAME_RESOURCE_FIELD_REF)
  private V1ScanSpecValueFromResourceFieldRef resourceFieldRef;

  public static final String SERIALIZED_NAME_SECRET_KEY_REF = "secretKeyRef";
  @SerializedName(SERIALIZED_NAME_SECRET_KEY_REF)
  private V1ScanSpecValueFromSecretKeyRef secretKeyRef;


  public V1ScanSpecValueFrom configMapKeyRef(V1ScanSpecValueFromConfigMapKeyRef configMapKeyRef) {
    
    this.configMapKeyRef = configMapKeyRef;
    return this;
  }

   /**
   * Get configMapKeyRef
   * @return configMapKeyRef
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public V1ScanSpecValueFromConfigMapKeyRef getConfigMapKeyRef() {
    return configMapKeyRef;
  }


  public void setConfigMapKeyRef(V1ScanSpecValueFromConfigMapKeyRef configMapKeyRef) {
    this.configMapKeyRef = configMapKeyRef;
  }


  public V1ScanSpecValueFrom fieldRef(V1ScanSpecValueFromFieldRef fieldRef) {
    
    this.fieldRef = fieldRef;
    return this;
  }

   /**
   * Get fieldRef
   * @return fieldRef
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public V1ScanSpecValueFromFieldRef getFieldRef() {
    return fieldRef;
  }


  public void setFieldRef(V1ScanSpecValueFromFieldRef fieldRef) {
    this.fieldRef = fieldRef;
  }


  public V1ScanSpecValueFrom resourceFieldRef(V1ScanSpecValueFromResourceFieldRef resourceFieldRef) {
    
    this.resourceFieldRef = resourceFieldRef;
    return this;
  }

   /**
   * Get resourceFieldRef
   * @return resourceFieldRef
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public V1ScanSpecValueFromResourceFieldRef getResourceFieldRef() {
    return resourceFieldRef;
  }


  public void setResourceFieldRef(V1ScanSpecValueFromResourceFieldRef resourceFieldRef) {
    this.resourceFieldRef = resourceFieldRef;
  }


  public V1ScanSpecValueFrom secretKeyRef(V1ScanSpecValueFromSecretKeyRef secretKeyRef) {
    
    this.secretKeyRef = secretKeyRef;
    return this;
  }

   /**
   * Get secretKeyRef
   * @return secretKeyRef
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public V1ScanSpecValueFromSecretKeyRef getSecretKeyRef() {
    return secretKeyRef;
  }


  public void setSecretKeyRef(V1ScanSpecValueFromSecretKeyRef secretKeyRef) {
    this.secretKeyRef = secretKeyRef;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1ScanSpecValueFrom v1ScanSpecValueFrom = (V1ScanSpecValueFrom) o;
    return Objects.equals(this.configMapKeyRef, v1ScanSpecValueFrom.configMapKeyRef) &&
        Objects.equals(this.fieldRef, v1ScanSpecValueFrom.fieldRef) &&
        Objects.equals(this.resourceFieldRef, v1ScanSpecValueFrom.resourceFieldRef) &&
        Objects.equals(this.secretKeyRef, v1ScanSpecValueFrom.secretKeyRef);
  }

  @Override
  public int hashCode() {
    return Objects.hash(configMapKeyRef, fieldRef, resourceFieldRef, secretKeyRef);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1ScanSpecValueFrom {\n");
    sb.append("    configMapKeyRef: ").append(toIndentedString(configMapKeyRef)).append("\n");
    sb.append("    fieldRef: ").append(toIndentedString(fieldRef)).append("\n");
    sb.append("    resourceFieldRef: ").append(toIndentedString(resourceFieldRef)).append("\n");
    sb.append("    secretKeyRef: ").append(toIndentedString(secretKeyRef)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

}

