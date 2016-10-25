/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.query.routing;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPTableScan;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ModelChooser {

    public static Set<IRealization> selectModel(OLAPContext context) {
        Map<DataModelDesc, Set<IRealization>> modelMap = makeOrderedModelMap(context);
        OLAPTableScan firstTable = context.firstTableScan;
        List<JoinDesc> joins = context.joins;

        for (DataModelDesc model : modelMap.keySet()) {
            Map<String, String> aliasMap = matches(model, firstTable, joins);
            if (aliasMap != null) {
                fixModel(context, model, aliasMap);
                return modelMap.get(model);
            }
        }
        
        throw new NoRealizationFoundException("No model found by first table " + firstTable.getOlapTable().getTableName() + " and joins " + joins);
    }

    private static Map<String, String> matches(DataModelDesc model, OLAPTableScan firstTable, List<JoinDesc> joins) {
        Map<String, String> result = Maps.newHashMap();
        
        // no join special case
        if (joins.isEmpty()) {
            TableRef tableRef = model.findFirstTable(firstTable.getOlapTable().getTableName());
            if (tableRef == null)
                return null;
            result.put(firstTable.getAlias(), tableRef.getAlias());
            return result;
        }
        
        // the greedy match is not perfect but works for the moment
        
        return null;
    }

    private static Map<DataModelDesc, Set<IRealization>> makeOrderedModelMap(OLAPContext context) {
        KylinConfig kylinConfig = context.olapSchema.getConfig();
        String projectName = context.olapSchema.getProjectName();
        String factTableName = context.firstTableScan.getOlapTable().getTableName();
        Set<IRealization> realizations = ProjectManager.getInstance(kylinConfig).getRealizationsByTable(projectName, factTableName);

        final Map<DataModelDesc, Set<IRealization>> models = Maps.newHashMap();
        final Map<DataModelDesc, MutableLong> endTimes = Maps.newHashMap();
        for (IRealization real : realizations) {
            if (real.isReady() == false)
                continue;

            DataModelDesc m = real.getDataModelDesc();
            Set<IRealization> set = models.get(m);
            if (set == null) {
                set = Sets.newHashSet();
                set.add(real);
                models.put(m, set);
                endTimes.put(m, new MutableLong(real.getDateRangeEnd()));
            } else {
                set.add(real);
                MutableLong endTime = endTimes.get(m);
                endTime.setValue(Math.max(real.getDateRangeEnd(), endTime.longValue()));
            }
        }

        // order by end time desc
        TreeMap<DataModelDesc, Set<IRealization>> result = Maps.newTreeMap(new Comparator<DataModelDesc>() {
            @Override
            public int compare(DataModelDesc o1, DataModelDesc o2) {
                long comp = endTimes.get(o2).longValue() - endTimes.get(o1).longValue();
                return comp == 0 ? 0 : (comp < 0 ? -1 : 1);
            }
        });
        result.putAll(models);

        return result;
    }

    private static void fixModel(OLAPContext context, DataModelDesc model, Map<String, String> aliasMap) {
        for (OLAPTableScan tableScan : context.allTableScans) {
            tableScan.fixColumnRowTypeWithModel(model, aliasMap);
        }
    }
}
