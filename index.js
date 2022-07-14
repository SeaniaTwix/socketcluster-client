import { AGClientSocket } from './lib/clientsocket';
import factory from './lib/factory';
const version = '16.0.4';

export default {
  factory,
  AGClientSocket,
  create: function (options) {
    return factory.create({...options, version});
  },
  version,
};
