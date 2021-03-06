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

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.bin.Bin;
import org.apache.nifi.processor.util.bin.BinFiles;
import org.apache.nifi.processor.util.bin.BinManager;
import org.apache.nifi.processor.util.bin.BinProcessingResult;
import org.apache.nifi.processors.standard.merge.AttributeStrategy;
import org.apache.nifi.processors.standard.merge.AttributeStrategyUtil;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import com.backblaze.erasure.ReedSolomon;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@ReadsAttributes({
        @ReadsAttribute(attribute = "fragment.identifier", description = "All FlowFiles with the same value for this attribute will be bundled together."),
        @ReadsAttribute(attribute = "fragment.count", description = "Number of possible fragments that were sent."),
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
                + "FlowFile."),
        @ReadsAttribute(attribute = "fragment.data.count", description = "The number of data shards to expect for a FlowFile"),
        @ReadsAttribute(attribute = "fragment.parity.count", description = "The number of parity shards to expect for a FlowFile")
})
@WritesAttributes({
        @WritesAttribute(attribute = "merge.count", description = "The number of FlowFiles that were merged into this bundle"),
        @WritesAttribute(attribute = "merge.bin.age", description = "The age of the bin, in milliseconds, when it was merged and output. Effectively "
                + "this is the greatest amount of time that any FlowFile in this bundle remained waiting in this processor before it was output"),
        @WritesAttribute(attribute = "merge.uuid", description = "UUID of the merged flow file that will be added to the original flow files attributes.")
})
public class MergeParity extends BinFiles {

    public static final String FRAGMENT_ID_ATTRIBUTE = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX_ATTRIBUTE = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT_ATTRIBUTE = FragmentAttributes.FRAGMENT_COUNT.key();
    public static final String FRAGMENT_DATA_COUNT = "fragment.data.count";
    public static final String FRAGMENT_PARITY_COUNT = "fragment.parity.count";

    public static final PropertyDescriptor DATA_SHARDS = new PropertyDescriptor
            .Builder().name("DATA_SHARDS")
            .displayName("Data Shards")
            .description("Number of Data Shards needed to Parity Merge.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CORRELATION_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Correlation Attribute Name")
            .description("If specified, like FlowFiles will be binned together, where 'like FlowFiles' means FlowFiles that have the same value for "
                    + "this Attribute. If not specified, FlowFiles are bundled by the order in which they are pulled from the queue.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .defaultValue(null)
            .build();

    // old style attributes
    public static final String SEGMENT_ORIGINAL_FILENAME = FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key();

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

    public static final String MERGE_COUNT_ATTRIBUTE = "merge.count";
    public static final String MERGE_BIN_AGE_ATTRIBUTE = "merge.bin.age";
    public static final String MERGE_UUID_ATTRIBUTE = "merge.uuid";

    public static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DATA_SHARDS);
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

    @Override
    protected FlowFile preprocessFlowFile(ProcessContext processContext, ProcessSession processSession, FlowFile flowFile) {
        return flowFile;
    }

    @Override
    protected String getGroupId(ProcessContext processContext, FlowFile flowFile, ProcessSession processSession) {
        final String correlationAttributeName = processContext.getProperty(CORRELATION_ATTRIBUTE_NAME)
                .evaluateAttributeExpressions(flowFile).getValue();
        String groupId = correlationAttributeName == null ? null : flowFile.getAttribute(correlationAttributeName);

        if (groupId == null) {
            groupId = flowFile.getAttribute(FRAGMENT_ID_ATTRIBUTE);
        }

        return groupId;
    }

    @Override
    protected void setUpBinManager(BinManager binManager, ProcessContext processContext) {
        binManager.setFileCountAttribute(FRAGMENT_COUNT_ATTRIBUTE);
    }

    @Override
    protected BinProcessingResult processBin(Bin bin, ProcessContext processContext) throws ProcessException {
        final BinProcessingResult binProcessingResult = new BinProcessingResult(true);

        MergeBin merger = new ParityMerge();
        final AttributeStrategy attributeStrategy = AttributeStrategyUtil.strategyFor(processContext);
        final List<FlowFile> contents = bin.getContents();
        final ProcessSession binSession = bin.getSession();

        final String dataShardsSize = processContext.getProperty(DATA_SHARDS).getValue();
        if (!isNumber(dataShardsSize)) {
            getLogger().error("Could not determine data shard size, it is not a valid number.");
            throw new AssertionError();
        }

        final String error = getParityMergeValidationError(bin.getContents(), Integer.valueOf(dataShardsSize));
        // Fail the flow files and commit them
        if (error != null) {
            final String binDescription = contents.size() <= 10 ? contents.toString() : contents.size() + " FlowFiles";
            getLogger().error(error + "; routing {} to failure", new Object[]{binDescription});
            binSession.transfer(contents, REL_FAILURE);
            binSession.commit();
            return binProcessingResult;
        }

        Collections.sort(contents);
        FlowFile bundle = merger.merge(bin, processContext);

        // keep the filename, as it is added to the bundle.
        final String filename = bundle.getAttribute(CoreAttributes.FILENAME.key());

        // merge all of the attributes
        final Map<String, String> bundleAttributes = attributeStrategy.getMergedAttributes(contents);
        bundleAttributes.put(CoreAttributes.MIME_TYPE.key(), merger.getMergedContentType());
        // restore the filename of the bundle
        bundleAttributes.put(CoreAttributes.FILENAME.key(), filename);
        bundleAttributes.put(MERGE_COUNT_ATTRIBUTE, Integer.toString(contents.size()));
        bundleAttributes.put(MERGE_BIN_AGE_ATTRIBUTE, Long.toString(bin.getBinAge()));

        bundle = binSession.putAllAttributes(bundle, bundleAttributes);

        final String inputDescription = contents.size() < 10 ? contents.toString() : contents.size() + " FlowFiles";
        getLogger().info("Merged {} into {}", new Object[]{inputDescription, bundle});
        binSession.transfer(bundle, REL_MERGED);
        binProcessingResult.getAttributes().put(MERGE_UUID_ATTRIBUTE, bundle.getAttribute(CoreAttributes.UUID.key()));

        for (final FlowFile unmerged : merger.getUnmergedFlowFiles()) {
            final FlowFile unmergedCopy = binSession.clone(unmerged);
            binSession.transfer(unmergedCopy, REL_FAILURE);
        }

        // We haven't committed anything, parent will take care of it
        binProcessingResult.setCommitted(false);
        return binProcessingResult;

    }

    private String getParityMergeValidationError(final List<FlowFile> binContents, int dataShardSize) {
        if (binContents.isEmpty()) {
            return null;
        }

        // All fragments must have the appropriate attributes.
        String decidedFragmentCount = null;
        String fragmentIdentifier = null;
        for (final FlowFile flowFile : binContents) {
            final String fragmentIndex = flowFile.getAttribute(FRAGMENT_INDEX_ATTRIBUTE);
            if (!isNumber(fragmentIndex)) {
                return "Cannot Parity Merge " + flowFile + " because it does not have an integer value for the " + FRAGMENT_INDEX_ATTRIBUTE + " attribute";
            }

            fragmentIdentifier = flowFile.getAttribute(FRAGMENT_ID_ATTRIBUTE);

            final String fragmentCount = flowFile.getAttribute(FRAGMENT_COUNT_ATTRIBUTE);
            if (!isNumber(fragmentCount)) {
                return "Cannot Parity Merge " + flowFile + " because it does not have an integer value for the " + FRAGMENT_COUNT_ATTRIBUTE + " attribute";
            } else if (decidedFragmentCount == null) {
                decidedFragmentCount = fragmentCount;
            } else if (!decidedFragmentCount.equals(fragmentCount)) {
                return "Cannot Parity Merge " + flowFile + " because it is grouped with another FlowFile, and the two have differing values for the "
                        + FRAGMENT_COUNT_ATTRIBUTE + " attribute: " + decidedFragmentCount + " and " + fragmentCount;
            }

            String dataShardCount = flowFile.getAttribute(FRAGMENT_DATA_COUNT);
            if (!isNumber(dataShardCount)) {
                return "Cannot Parity Merge " + flowFile + " because it does not have an integer value for the " + FRAGMENT_DATA_COUNT + " attribute";
            }

            String parityShardCount = flowFile.getAttribute(FRAGMENT_PARITY_COUNT);
            if (!isNumber(parityShardCount)) {
                return "Cannot Parity Merge " + flowFile + " because it does not have an integer value for the " + FRAGMENT_PARITY_COUNT + " attribute";
            }

        }

        // If the bin doesn't have enough shards to parity merge, can't continue
        if (binContents.size() < dataShardSize) {
            return "Cannot Parity Merge FlowFiles with Fragment Identifier " + fragmentIdentifier + " because only " + binContents.size() + " data shards exist and " + dataShardSize + " are required.";
        }

        final int numericFragmentCount;
        try {
            numericFragmentCount = Integer.parseInt(decidedFragmentCount);
        } catch (final NumberFormatException nfe) {
            return "Cannot ParityMerge FlowFiles with Fragment Identifier " + fragmentIdentifier + " because the " + FRAGMENT_COUNT_ATTRIBUTE + " has a non-integer value of " + decidedFragmentCount;
        }

        if (binContents.size() > numericFragmentCount) {
            return "Cannot ParityMerge FlowFiles with Fragment Identifier " + fragmentIdentifier + " because the expected number of fragments is " + decidedFragmentCount + " but found "
                    + binContents.size() + " fragments for this identifier";
        }

        return null;
    }

    private byte[] readContent(final String filename) throws IOException {
        return Files.readAllBytes(Paths.get(filename));
    }

    private boolean isNumber(final String value) {
        if (value == null) {
            return false;
        }

        return NUMBER_PATTERN.matcher(value).matches();
    }

    private String createFilename(final List<FlowFile> flowFiles) {
        if (flowFiles.size() == 1) {
            return flowFiles.get(0).getAttribute(CoreAttributes.FILENAME.key());
        } else {
            final FlowFile ff = flowFiles.get(0);
            final String origFilename = ff.getAttribute(SEGMENT_ORIGINAL_FILENAME);
            if (origFilename != null) {
                return origFilename;
            } else {
                return String.valueOf(System.nanoTime());
            }
        }
    }

    private interface MergeBin {
        FlowFile merge(Bin bin, ProcessContext context);
        String getMergedContentType();
        List<FlowFile> getUnmergedFlowFiles();
    }

    private class ParityMerge implements MergeBin {

        private String mimeType = "application/octet-stream";

        public ParityMerge() {

        }

        @Override
        public FlowFile merge(final Bin bin, final ProcessContext context) {
            final List<FlowFile> contents = bin.getContents();
            final Iterator<FlowFile> itr = contents.iterator();

            final ProcessSession session = bin.getSession();
            final int totalShards = Integer.getInteger(contents.get(0).getAttribute(FRAGMENT_COUNT_ATTRIBUTE));
            final int dataShardSize = Integer.getInteger(contents.get(0).getAttribute(FRAGMENT_DATA_COUNT));
            final int parityShardSize  = Integer.getInteger(contents.get(0).getAttribute(FRAGMENT_PARITY_COUNT));
//            final int dataShardsSize = context.getProperty(DATA_SHARDS).asInteger();

            AtomicBoolean error = new AtomicBoolean();
            FlowFile outputFlowFile = session.create();

            try {

                final byte[][] shards = new byte[totalShards][];

                final boolean[] shardPresent = new boolean[totalShards];
                int shardSize = 0;
                int shardCount = 0;
                int fragmentIdx = 0;
                while (itr.hasNext()) {
                    final FlowFile flowFile = itr.next();
                    int flowFragmentIdx = Integer.getInteger(flowFile.getAttribute(FRAGMENT_INDEX_ATTRIBUTE));

                    if (fragmentIdx == flowFragmentIdx) {
                        // A fragment was received
                        shardSize = (int) flowFile.getSize();
                        shards[fragmentIdx] = new byte[shardSize];
                        shardPresent[fragmentIdx] = true;
                        shardCount += 1;

                        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                        session.exportTo(flowFile, byteStream);
                        byte[] tmpBytes = byteStream.toByteArray();
                        System.arraycopy(tmpBytes, 0, shards[fragmentIdx], 0, tmpBytes.length);
                    }
                }

                // We need at least DATA_SHARDS to be able to reconstruct the file.
                if (shardCount < dataShardSize) {
                    error.set(true);
                    getLogger().error("Not enough shards present.");
                }

                // Make empty buffers for the missing shards.
                for (int i = 0; i < totalShards; i++) {
                    if (!shardPresent[i]) {
                        shards[i] = new byte[shardSize];
                    }
                }

                // Use Reed-Solomon to fill in the missing shards
                ReedSolomon reedSolomon = ReedSolomon.create(dataShardSize, parityShardSize);
                reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);

                // Combine the data shards into one buffer for convenience.
                // (This is not efficient, but it is convenient.)
                byte[] allBytes = new byte[shardSize * dataShardSize];
                for (int i = 0; i < dataShardSize; i++) {
                    System.arraycopy(shards[i], 0, allBytes, shardSize * i, shardSize);
                }

                session.write(outputFlowFile, out -> out.write(allBytes));
            } catch (Exception e) {
                error.set(true);
            }
            return outputFlowFile;
        }

        @Override
        public String getMergedContentType() {
            return mimeType;
        }

        @Override
        public List<FlowFile> getUnmergedFlowFiles() {
            return Collections.emptyList();
        }
    }

    private static class FragmentComparator implements Comparator<FlowFile> {

        @Override
        public int compare(final FlowFile o1, final FlowFile o2) {
            final int fragmentIndex1 = Integer.parseInt(o1.getAttribute(FRAGMENT_INDEX_ATTRIBUTE));
            final int fragmentIndex2 = Integer.parseInt(o2.getAttribute(FRAGMENT_INDEX_ATTRIBUTE));
            return Integer.compare(fragmentIndex1, fragmentIndex2);
        }
    }
    
}
