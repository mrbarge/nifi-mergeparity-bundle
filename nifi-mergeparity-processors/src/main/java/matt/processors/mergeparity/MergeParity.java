/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package matt.processors.mergeparity;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@ReadsAttributes({
        @ReadsAttribute(attribute = "fragment.identifier", description = "All FlowFiles with the same value for this attribute will be bundled together."),
        @ReadsAttribute(attribute = "fragment.index", description = "This attribute indicates the order in which the fragments should be assembled. This "
                + "attribute must be present on all FlowFiles and must be a unique (i.e., unique across all "
                + "FlowFiles that have the same value for the \"fragment.identifier\" attribute) integer "
                + "between 0 and the value of the fragment.count attribute. If two or more FlowFiles have the same value for the "
                + "\"fragment.identifier\" attribute and the same value for the \"fragment.index\" attribute, the first FlowFile processed will be "
                + "accepted and subsequent FlowFiles will not be accepted into the Bin."),
        @ReadsAttribute(attribute = "fragment.count", description = "This attribute must be present on all FlowFiles with the same value for the fragment.identifier attribute. All FlowFiles in the same "
                + "bundle must have the same value for this attribute. The value of this attribute indicates how many FlowFiles should be expected "
                + "in the given bundle."),
        @ReadsAttribute(attribute = "segment.original.filename", description = "This attribute must be present on all FlowFiles with the same value for the fragment.identifier attribute. All FlowFiles in the same "
                + "bundle must have the same value for this attribute. The value of this attribute will be used for the filename of the completed merged "
                + "FlowFile."),
        @ReadsAttribute(attribute = "segment.original.filename", description = "This attribute must be present on all FlowFiles with the same value for the fragment.identifier attribute. All FlowFiles in the same "
                + "bundle must have the same value for this attribute. The value of this attribute will be used for the filename of the completed merged "
                + "FlowFile.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "merge.count", description = "The number of FlowFiles that were merged into this bundle"),
        @WritesAttribute(attribute = "merge.bin.age", description = "The age of the bin, in milliseconds, when it was merged and output. Effectively "
                + "this is the greatest amount of time that any FlowFile in this bundle remained waiting in this processor before it was output"),
        @WritesAttribute(attribute = "merge.uuid", description = "UUID of the merged flow file that will be added to the original flow files attributes.")
})
public class MergeParity extends AbstractProcessor {

    public static final PropertyDescriptor DATA_SHARDS = new PropertyDescriptor
            .Builder().name("DATA_SHARDS")
            .displayName("Data Shards")
            .description("Number of Data Shards to split the file into.")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor PARITY_SHARDS = new PropertyDescriptor
            .Builder().name("PARITY_SHARDS")
            .displayName("Parity Shards")
            .description("Number of Parity Shards to generate for the file.")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    static final Relationship REL_MERGED = new Relationship.Builder()
            .name("merged")
            .description("The merged file.")
            .build();
    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original flow file.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed flow files.")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DATA_SHARDS);
        descriptors.add(PARITY_SHARDS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_MERGED);
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        // TODO implement
    }
}
