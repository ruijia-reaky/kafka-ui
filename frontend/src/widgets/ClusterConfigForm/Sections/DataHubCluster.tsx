import React from 'react';
import Input from 'components/common/Input/Input';
import Heading from 'components/common/heading/Heading.styled';
import Checkbox from 'components/common/Checkbox/Checkbox';

const DataHubCluster: React.FC = () => (
  <>
    <Heading level={3}>DataHub Cluster</Heading>
    <Input
      label="Cluster name *"
      type="text"
      name="name"
      withError
      hint="this name will help you recognize the cluster in the application interface"
    />
    <Checkbox
      name="readOnly"
      label="Read-only mode"
      hint="allows you to run an application in read-only mode for a specific cluster"
    />
    <Input
      label="Endpoint *"
      type="text"
      name="datahub.endpoint"
      placeholder="https://dh-cn-hangzhou.aliyuncs.com"
      withError
      hint="DataHub service endpoint of the region"
    />
    <Input
      label="Projects *"
      type="text"
      name="datahub.projects"
      placeholder="project_a, project_b"
      withError
      hint="comma-separated DataHub projects to show in this cluster"
    />
    <Input
      label="AccessKey ID *"
      type="text"
      name="datahub.accessId"
      withError
    />
    <Input
      label="AccessKey Secret *"
      type="password"
      name="datahub.accessKey"
      withError
      hint="the saved secret is shown masked; keep it to leave the secret unchanged"
    />
    <Input
      label="Security Token"
      type="password"
      name="datahub.securityToken"
      withError
      hint="only for STS temporary credentials"
    />
  </>
);

export default DataHubCluster;
