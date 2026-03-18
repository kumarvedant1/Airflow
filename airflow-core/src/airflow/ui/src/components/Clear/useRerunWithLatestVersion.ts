/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { useEffect, useRef, useState } from "react";

import { useConfig } from "src/queries/useConfig";

type UseRerunWithLatestVersionProps = {
  /** DAG-level rerun_with_latest_version from DAG details. */
  dagLevelConfig?: boolean | null;
};

type UseRerunWithLatestVersionResult = {
  /**
   * The resolved value. `undefined` means config has not loaded yet;
   * the caller should omit run_on_latest_version from the request
   * so the backend hierarchy applies.
   */
  setValue: (newValue: boolean) => void;
  value: boolean | undefined;
};

/**
 * Resolves the default checkbox state for "Run on Latest Version".
 * Precedence: DAG-level > global config > false.
 *
 * Returns `undefined` until the config is resolved, so the clear
 * request can omit the field and let the backend hierarchy decide.
 * Once resolved (or if the user toggles the checkbox), returns a boolean.
 */
export const useRerunWithLatestVersion = ({
  dagLevelConfig,
}: UseRerunWithLatestVersionProps): UseRerunWithLatestVersionResult => {
  const globalConfigValue = useConfig("rerun_with_latest_version") as boolean | undefined;

  const resolvedDefault = dagLevelConfig ?? globalConfigValue ?? false;

  const [value, setValue] = useState<boolean | undefined>(undefined);
  const userHasToggled = useRef(false);

  useEffect(() => {
    if (!userHasToggled.current && dagLevelConfig !== undefined) {
      setValue(resolvedDefault);
    }
  }, [dagLevelConfig, globalConfigValue, resolvedDefault]);

  const handleSetValue = (newValue: boolean) => {
    userHasToggled.current = true;
    setValue(newValue);
  };

  return { setValue: handleSetValue, value };
};
