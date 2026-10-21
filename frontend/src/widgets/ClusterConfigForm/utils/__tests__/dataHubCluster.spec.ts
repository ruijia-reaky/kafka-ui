import { ApplicationConfigPropertiesKafkaClustersTypeEnum } from 'generated-sources';
import formSchema from 'widgets/ClusterConfigForm/schema';
import { ClusterConfigFormValues } from 'widgets/ClusterConfigForm/types';
import { getInitialFormData } from 'widgets/ClusterConfigForm/utils/getInitialFormData';
import { transformFormDataToPayload } from 'widgets/ClusterConfigForm/utils/transformFormDataToPayload';

const { DATAHUB, KAFKA } = ApplicationConfigPropertiesKafkaClustersTypeEnum;

const dataHubForm: ClusterConfigFormValues = {
  name: 'datahub-hz',
  readOnly: true,
  type: DATAHUB,
  datahub: {
    endpoint: 'https://dh.example.com',
    projects: ' project_a, ,project_b ',
    accessId: 'id',
    accessKey: '******',
    securityToken: '',
  },
  bootstrapServers: [{ host: '', port: '' }],
  customAuth: {},
};

describe('ClusterConfigForm DataHub support', () => {
  it('transforms DataHub form values to a DataHub cluster payload', () => {
    expect(transformFormDataToPayload(dataHubForm)).toEqual({
      name: 'datahub-hz',
      type: DATAHUB,
      readOnly: true,
      datahub: {
        endpoint: 'https://dh.example.com',
        projects: ['project_a', 'project_b'],
        accessId: 'id',
        accessKey: '******',
        securityToken: undefined,
      },
    });
  });

  it('round-trips a DataHub cluster through the form', () => {
    const payload = transformFormDataToPayload(dataHubForm);
    const formData = getInitialFormData(payload);

    expect(formData.type).toBe(DATAHUB);
    expect(formData.datahub?.projects).toBe('project_a, project_b');
    expect(transformFormDataToPayload(formData)).toEqual(payload);
  });

  it('marks Kafka clusters with the KAFKA type', () => {
    expect(
      getInitialFormData({ name: 'kafka', bootstrapServers: 'h:9092' }).type
    ).toBe(KAFKA);
  });

  it('does not require bootstrap servers for DataHub clusters', async () => {
    await expect(formSchema.validate(dataHubForm)).resolves.toBeTruthy();
  });

  it('requires DataHub connection fields', async () => {
    await expect(
      formSchema.validate({
        ...dataHubForm,
        datahub: { ...dataHubForm.datahub, accessId: '' },
      })
    ).rejects.toThrow('required field');
  });

  it('still requires bootstrap servers for Kafka clusters', async () => {
    await expect(
      formSchema.validate({ ...dataHubForm, type: KAFKA, bootstrapServers: [] })
    ).rejects.toThrow();
  });
});
